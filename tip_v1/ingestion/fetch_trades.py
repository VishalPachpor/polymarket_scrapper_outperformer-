from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.canonical import (
    coalesce,
    normalize_decimal_text,
    normalize_outcome,
    normalize_side,
    normalize_timestamp,
)
from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class IngestionResult:
    wallet: str
    run_id: int
    fetched_count: int
    inserted_count: int
    duplicate_count: int
    last_cursor: str
    status: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _core_fingerprint_fields(wallet: str, trade: dict[str, Any]) -> dict[str, Any]:
    raw_outcome = coalesce(trade, "outcome")
    normalized_outcome = (
        normalize_outcome(raw_outcome) if raw_outcome not in (None, "") else None
    )
    raw_side = coalesce(trade, "side", "action")
    normalized_side = normalize_side(raw_side) if raw_side not in (None, "") else ""
    raw_price = coalesce(trade, "price")
    raw_size = coalesce(trade, "size")
    raw_timestamp = coalesce(trade, "timestamp")

    return {
        "wallet": wallet.lower(),
        "trade_id": coalesce(trade, "id", "tradeId"),
        "market_id": coalesce(trade, "conditionId", "market", "marketId"),
        "outcome": normalized_outcome,
        "asset": coalesce(trade, "asset", "tokenId"),
        "side": normalized_side,
        "price": normalize_decimal_text(raw_price) if raw_price not in (None, "") else "",
        "size": normalize_decimal_text(raw_size) if raw_size not in (None, "") else "",
        "timestamp": (
            str(normalize_timestamp(raw_timestamp))
            if raw_timestamp not in (None, "")
            else ""
        ),
    }


def make_trade_fingerprint(wallet: str, trade: dict[str, Any]) -> str:
    fingerprint = _core_fingerprint_fields(wallet, trade)
    fingerprint["transaction_hash"] = coalesce(trade, "transactionHash")
    payload = json.dumps(fingerprint, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def make_trade_identity(wallet: str, trade: dict[str, Any]) -> str:
    fingerprint = _core_fingerprint_fields(wallet, trade)
    payload = json.dumps(fingerprint, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fetch_trade_page(
    wallet: str,
    settings: Settings,
    *,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    params = {
        "user": wallet,
        "limit": limit,
        "offset": offset,
        "takerOnly": "true",
    }
    request = Request(
        f"{settings.trades_api_url}?{urlencode(params)}",
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1/0.1",
        },
    )

    with urlopen(request, timeout=settings.request_timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not isinstance(payload, list):
        raise ValueError("Trades API returned a non-list payload.")

    return payload


def start_ingestion_run(wallet: str, settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    with managed_connection(settings) as connection:
        cursor = connection.execute(
            """
            INSERT INTO ingestion_runs (wallet, started_at, status)
            VALUES (?, ?, ?)
            """,
            (wallet, _now_iso(), "RUNNING"),
        )
        connection.commit()
        return int(cursor.lastrowid)


def _default_page_overlap(limit: int, page_count: int) -> int:
    if page_count <= 1 or limit <= 1:
        return 0
    return max(1, min(limit - 1, limit // 5))


def _page_step(limit: int, overlap: int) -> int:
    step = limit - overlap
    if step <= 0:
        raise ValueError("Page overlap must be smaller than the page limit.")
    return step


def complete_ingestion_run(
    run_id: int,
    *,
    last_cursor: str,
    status: str,
    error_count: int,
    settings: Settings | None = None,
) -> None:
    settings = settings or get_settings()
    with managed_connection(settings) as connection:
        connection.execute(
            """
            UPDATE ingestion_runs
            SET completed_at = ?, last_cursor = ?, status = ?, error_count = ?
            WHERE id = ?
            """,
            (_now_iso(), last_cursor, status, error_count, run_id),
        )
        connection.commit()


def ingest_trade_payloads(
    wallet: str,
    payloads: list[dict[str, Any]],
    settings: Settings | None = None,
) -> tuple[int, int]:
    settings = settings or get_settings()
    inserted = 0
    duplicates = 0

    with managed_connection(settings) as connection:
        for trade in payloads:
            dedupe_key = make_trade_fingerprint(wallet, trade)
            raw_json = json.dumps(trade, sort_keys=True, separators=(",", ":"))
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO trades_raw (wallet, raw_json, dedupe_key)
                VALUES (?, ?, ?)
                """,
                (wallet, raw_json, dedupe_key),
            )
            inserted += cursor.rowcount
            duplicates += 1 - cursor.rowcount

        connection.commit()

    return inserted, duplicates


def ingest_trades(
    wallet: str,
    settings: Settings | None = None,
    *,
    page_limit: int | None = None,
    max_pages: int | None = None,
    page_overlap: int | None = None,
    stabilization_sweeps: int = 5,
    stable_sweep_threshold: int = 2,
    target_unique_count: int | None = None,
) -> IngestionResult:
    settings = settings or get_settings()
    limit = page_limit or settings.page_limit
    page_count = max_pages or settings.max_pages_per_run
    overlap = (
        _default_page_overlap(limit, page_count)
        if page_overlap is None
        else page_overlap
    )
    step = _page_step(limit, overlap)
    sweep_budget = max(1, stabilization_sweeps)
    required_stable = max(1, stable_sweep_threshold)

    run_id = start_ingestion_run(wallet, settings)
    fetched_count = 0
    inserted_count = 0
    duplicate_count = 0
    last_cursor = "0"
    error_count = 0

    try:
        stable_sweeps = 0

        for sweep_index in range(sweep_budget):
            sweep_inserted = 0
            short_page_seen = False

            for page_index in range(page_count):
                offset = page_index * step
                trades = fetch_trade_page(wallet, settings, limit=limit, offset=offset)
                fetched_count += len(trades)
                inserted, duplicates = ingest_trade_payloads(wallet, trades, settings)
                inserted_count += inserted
                duplicate_count += duplicates
                sweep_inserted += inserted
                last_cursor = f"sweep={sweep_index},offset={offset}"

                if len(trades) < limit:
                    short_page_seen = True
                    break

            if sweep_inserted == 0:
                stable_sweeps += 1
            else:
                stable_sweeps = 0

            if target_unique_count is not None:
                if inserted_count >= target_unique_count and stable_sweeps >= required_stable:
                    break
            elif stable_sweeps >= required_stable:
                break

            if short_page_seen and stable_sweeps >= required_stable:
                break

        complete_ingestion_run(
            run_id,
            last_cursor=last_cursor,
            status="SUCCESS",
            error_count=error_count,
            settings=settings,
        )
        return IngestionResult(
            wallet=wallet,
            run_id=run_id,
            fetched_count=fetched_count,
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            last_cursor=last_cursor,
            status="SUCCESS",
        )
    except (HTTPError, URLError, ValueError, json.JSONDecodeError):
        error_count += 1
        complete_ingestion_run(
            run_id,
            last_cursor=last_cursor,
            status="FAILED",
            error_count=error_count,
            settings=settings,
        )
        raise
