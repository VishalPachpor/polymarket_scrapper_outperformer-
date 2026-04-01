from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from tip_v1.canonical import (
    coalesce,
    normalize_decimal,
    normalize_decimal_text,
    normalize_outcome,
    normalize_side,
    normalize_timestamp,
)
from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection
from tip_v1.ingestion.fetch_trades import make_trade_identity


@dataclass(frozen=True)
class NormalizationResult:
    processed_count: int
    inserted_count: int
    skipped_count: int
    quarantined_count: int
    failed_count: int


@dataclass(frozen=True)
class NormalizedTrade:
    raw_trade_id: int
    trade_id: str
    wallet: str
    market_id: str
    outcome: str
    asset_id: str | None
    side: str
    price: str
    size: str
    timestamp: int


class NormalizationCollisionError(ValueError):
    """Raised when a canonical trade id points to conflicting trade facts."""


def _build_trade_id(wallet: str, trade: dict[str, Any]) -> str:
    source_trade_id = coalesce(trade, "id", "tradeId")
    if source_trade_id:
        return str(source_trade_id)

    return make_trade_identity(wallet, trade)


def _quarantine_raw_trade(
    connection,
    *,
    raw_trade_id: int,
    wallet: str,
    raw_json: str,
    reason: str,
) -> None:
    connection.execute(
        """
        INSERT INTO quarantined_trades (raw_trade_id, wallet, reason, payload)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(raw_trade_id) DO UPDATE SET
            wallet = excluded.wallet,
            reason = excluded.reason,
            payload = excluded.payload
        """,
        (raw_trade_id, wallet, reason, raw_json),
    )


def _assert_no_trade_id_collision(connection, normalized: NormalizedTrade) -> None:
    existing = connection.execute(
        """
        SELECT wallet, market_id, outcome, asset_id, side, price, size, timestamp
        FROM trade_events
        WHERE wallet = ? AND trade_id = ?
        """,
        (normalized.wallet, normalized.trade_id),
    ).fetchone()

    if existing is None:
        return

    same_identity = (
        existing["wallet"] == normalized.wallet
        and existing["market_id"] == normalized.market_id
        and existing["outcome"] == normalized.outcome
        and existing["asset_id"] == normalized.asset_id
        and existing["side"] == normalized.side
        and str(existing["price"]) == normalized.price
        and str(existing["size"]) == normalized.size
        and int(existing["timestamp"]) == normalized.timestamp
    )

    if not same_identity:
        raise NormalizationCollisionError(
            f"Canonical trade id collision for trade_id={normalized.trade_id}."
        )


def normalize_trade_payload(raw_trade_id: int, wallet: str, trade: dict[str, Any]) -> NormalizedTrade:
    market_id = coalesce(trade, "conditionId", "market", "marketId")
    side = normalize_side(coalesce(trade, "side", "action"))
    outcome = normalize_outcome(coalesce(trade, "outcome"))
    raw_price = coalesce(trade, "price")
    raw_size = coalesce(trade, "size")
    size_float = normalize_decimal(raw_size)
    timestamp = normalize_timestamp(coalesce(trade, "timestamp"))
    asset_id = coalesce(trade, "asset", "tokenId")

    if not market_id:
        raise ValueError("Missing market identifier.")
    if size_float <= 0:
        raise ValueError("Trade size must be positive.")

    return NormalizedTrade(
        raw_trade_id=raw_trade_id,
        trade_id=_build_trade_id(wallet, trade),
        wallet=wallet,
        market_id=str(market_id),
        outcome=outcome,
        asset_id=str(asset_id) if asset_id else None,
        side=side,
        price=normalize_decimal_text(raw_price),
        size=normalize_decimal_text(raw_size),
        timestamp=timestamp,
    )


def normalize_trades(
    wallet: str | None = None,
    settings: Settings | None = None,
    *,
    batch_size: int = 500,
) -> NormalizationResult:
    settings = settings or get_settings()

    processed_count = 0
    inserted_count = 0
    skipped_count = 0
    quarantined_count = 0
    failed_count = 0

    with managed_connection(settings) as connection:
        while True:
            query = """
                SELECT id, wallet, raw_json
                FROM trades_raw
                WHERE normalized_at IS NULL AND normalization_error IS NULL
            """
            params: list[Any] = []

            if wallet:
                query += " AND wallet = ?"
                params.append(wallet)

            query += " ORDER BY id LIMIT ?"
            params.append(batch_size)

            rows = connection.execute(query, params).fetchall()
            if not rows:
                break

            for row in rows:
                processed_count += 1

                try:
                    trade = json.loads(row["raw_json"])
                    normalized = normalize_trade_payload(row["id"], row["wallet"], trade)
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO trade_events (
                            raw_trade_id,
                            trade_id,
                            wallet,
                            market_id,
                            outcome,
                            asset_id,
                            side,
                            price,
                            size,
                            timestamp
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized.raw_trade_id,
                            normalized.trade_id,
                            normalized.wallet,
                            normalized.market_id,
                            normalized.outcome,
                            normalized.asset_id,
                            normalized.side,
                            normalized.price,
                            normalized.size,
                            normalized.timestamp,
                        ),
                    )
                    if cursor.rowcount == 0:
                        _assert_no_trade_id_collision(connection, normalized)
                    inserted_count += cursor.rowcount
                    skipped_count += 1 - cursor.rowcount
                    connection.execute(
                        """
                        UPDATE trades_raw
                        SET normalized_at = CURRENT_TIMESTAMP, normalization_error = NULL
                        WHERE id = ?
                        """,
                        (row["id"],),
                    )
                except (
                    TypeError,
                    ValueError,
                    json.JSONDecodeError,
                    NormalizationCollisionError,
                ) as exc:
                    failed_count += 1
                    quarantined_count += 1
                    connection.execute(
                        """
                        UPDATE trades_raw
                        SET normalization_error = ?
                        WHERE id = ?
                        """,
                        (str(exc), row["id"]),
                    )
                    _quarantine_raw_trade(
                        connection,
                        raw_trade_id=row["id"],
                        wallet=row["wallet"],
                        raw_json=row["raw_json"],
                        reason=str(exc),
                    )

            connection.commit()

    return NormalizationResult(
        processed_count=processed_count,
        inserted_count=inserted_count,
        skipped_count=skipped_count,
        quarantined_count=quarantined_count,
        failed_count=failed_count,
    )
