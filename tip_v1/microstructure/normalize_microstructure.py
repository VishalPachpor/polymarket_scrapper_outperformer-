from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from tip_v1.canonical import normalize_decimal
from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class MicrostructureNormalizationResult:
    processed_count: int
    inserted_count: int
    skipped_count: int


@dataclass(frozen=True)
class NormalizedBookSnapshot:
    raw_book_id: int
    snapshot_sequence_id: int
    asset_id: str
    market_id: str | None
    timestamp: int
    best_bid: float | None
    best_ask: float | None
    mid_price: float | None
    spread: float | None
    bid_depths: tuple[float, float, float]
    ask_depths: tuple[float, float, float]
    total_bid_depth: float
    total_ask_depth: float
    imbalance: float | None
    min_order_size: float | None
    tick_size: float | None
    neg_risk: int
    book_hash: str | None


def _normalize_snapshot_timestamp(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value)
    try:
        return int(text)
    except ValueError:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(parsed.astimezone(timezone.utc).timestamp())


def _normalize_depths(levels: list[dict], depth_levels: int) -> tuple[tuple[float, float, float], float]:
    normalized = [0.0, 0.0, 0.0]
    total = 0.0
    for level in levels:
        size = normalize_decimal(level.get("size", 0))
        total += size
    for index in range(min(depth_levels, 3, len(levels))):
        normalized[index] = normalize_decimal(levels[index].get("size", 0))
    return (normalized[0], normalized[1], normalized[2]), total


def normalize_book_payload(
    raw_book_id: int,
    payload: dict,
    *,
    depth_levels: int,
) -> NormalizedBookSnapshot:
    asset_id = str(payload["asset_id"])
    bids = payload.get("bids", [])
    asks = payload.get("asks", [])
    best_bid = normalize_decimal(bids[0]["price"]) if bids else None
    best_ask = normalize_decimal(asks[0]["price"]) if asks else None
    mid_price = None if best_bid is None or best_ask is None else (best_bid + best_ask) / 2
    spread = None if best_bid is None or best_ask is None else best_ask - best_bid
    bid_depths, total_bid_depth = _normalize_depths(bids, depth_levels)
    ask_depths, total_ask_depth = _normalize_depths(asks, depth_levels)
    total_depth = total_bid_depth + total_ask_depth
    imbalance = None if total_depth <= 0 else total_bid_depth / total_depth

    if spread is not None and spread < -1e-12:
        raise ValueError(
            f"Invalid orderbook state for asset_id={asset_id}: negative spread {spread}."
        )

    return NormalizedBookSnapshot(
        raw_book_id=raw_book_id,
        snapshot_sequence_id=raw_book_id,
        asset_id=asset_id,
        market_id=str(payload["market"]) if payload.get("market") else None,
        timestamp=_normalize_snapshot_timestamp(payload["timestamp"]),
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid_price,
        spread=spread,
        bid_depths=bid_depths,
        ask_depths=ask_depths,
        total_bid_depth=total_bid_depth,
        total_ask_depth=total_ask_depth,
        imbalance=imbalance,
        min_order_size=(
            normalize_decimal(payload["min_order_size"])
            if payload.get("min_order_size") not in (None, "")
            else None
        ),
        tick_size=(
            normalize_decimal(payload["tick_size"])
            if payload.get("tick_size") not in (None, "")
            else None
        ),
        neg_risk=1 if bool(payload.get("neg_risk")) else 0,
        book_hash=str(payload["hash"]) if payload.get("hash") else None,
    )


def normalize_book_snapshots(
    settings: Settings | None = None,
    *,
    batch_size: int = 500,
) -> MicrostructureNormalizationResult:
    settings = settings or get_settings()
    processed_count = 0
    inserted_count = 0
    skipped_count = 0

    with managed_connection(settings) as connection:
        while True:
            rows = connection.execute(
                """
                SELECT raw.id, raw.raw_payload
                FROM microstructure_raw_book AS raw
                LEFT JOIN microstructure_book_snapshots AS snapshot
                    ON snapshot.raw_book_id = raw.id
                WHERE snapshot.raw_book_id IS NULL
                ORDER BY raw.id
                LIMIT ?
                """,
                (batch_size,),
            ).fetchall()
            if not rows:
                break

            for row in rows:
                processed_count += 1
                payload = json.loads(row["raw_payload"])
                normalized = normalize_book_payload(
                    int(row["id"]),
                    payload,
                    depth_levels=settings.microstructure_depth_levels,
                )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO microstructure_book_snapshots (
                        snapshot_sequence_id,
                        raw_book_id,
                        asset_id,
                        market_id,
                        timestamp,
                        best_bid,
                        best_ask,
                        mid_price,
                        spread,
                        bid_depth_1,
                        bid_depth_2,
                        bid_depth_3,
                        ask_depth_1,
                        ask_depth_2,
                        ask_depth_3,
                        total_bid_depth,
                        total_ask_depth,
                        imbalance,
                        min_order_size,
                        tick_size,
                        neg_risk,
                        book_hash
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized.snapshot_sequence_id,
                        normalized.raw_book_id,
                        normalized.asset_id,
                        normalized.market_id,
                        normalized.timestamp,
                        normalized.best_bid,
                        normalized.best_ask,
                        normalized.mid_price,
                        normalized.spread,
                        normalized.bid_depths[0],
                        normalized.bid_depths[1],
                        normalized.bid_depths[2],
                        normalized.ask_depths[0],
                        normalized.ask_depths[1],
                        normalized.ask_depths[2],
                        normalized.total_bid_depth,
                        normalized.total_ask_depth,
                        normalized.imbalance,
                        normalized.min_order_size,
                        normalized.tick_size,
                        normalized.neg_risk,
                        normalized.book_hash,
                    ),
                )
                inserted_count += cursor.rowcount
                skipped_count += 1 - cursor.rowcount

            connection.commit()

    return MicrostructureNormalizationResult(
        processed_count=processed_count,
        inserted_count=inserted_count,
        skipped_count=skipped_count,
    )
