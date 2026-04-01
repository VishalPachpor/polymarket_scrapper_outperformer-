from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class MicrostructureIngestionResult:
    assets_requested: int
    fetched_count: int
    inserted_count: int
    duplicate_count: int


def fetch_orderbook(asset_id: str, settings: Settings) -> dict | None:
    request = Request(
        f"{settings.orderbook_api_url}?{urlencode({'token_id': asset_id})}",
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1/0.1",
        },
    )
    try:
        with urlopen(request, timeout=settings.request_timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise

    if not isinstance(payload, dict):
        raise ValueError("Orderbook API returned a non-object payload.")

    return payload


def fetch_orderbooks(asset_ids: list[str], settings: Settings) -> list[dict]:
    if not asset_ids:
        return []

    request = Request(
        settings.orderbooks_api_url,
        data=json.dumps(
            [
                {
                    "token_id": asset_id,
                }
                for asset_id in asset_ids
            ]
        ).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "tip-v1/0.1",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.request_timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            return []
        raise

    if not isinstance(payload, list):
        raise ValueError("Orderbooks API returned a non-list payload.")

    return payload


def _make_source_hash(payload: dict) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _normalize_observed_at(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value)
    try:
        return int(text)
    except ValueError:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(parsed.astimezone(timezone.utc).timestamp())


def ingest_orderbook_payloads(
    payloads: list[dict],
    settings: Settings | None = None,
) -> tuple[int, int]:
    settings = settings or get_settings()
    inserted = 0
    duplicates = 0

    with managed_connection(settings) as connection:
        for payload in payloads:
            asset_id = str(payload["asset_id"])
            observed_at = _normalize_observed_at(payload["observed_at"])
            source_hash = _make_source_hash(payload["payload"])
            raw_json = json.dumps(payload["payload"], sort_keys=True, separators=(",", ":"))

            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO microstructure_raw_book (
                    asset_id,
                    observed_at,
                    source_hash,
                    raw_payload
                )
                VALUES (?, ?, ?, ?)
                """,
                (asset_id, observed_at, source_hash, raw_json),
            )
            inserted += cursor.rowcount
            duplicates += 1 - cursor.rowcount

        connection.commit()

    return inserted, duplicates


def ingest_orderbooks_for_assets(
    asset_ids: list[str],
    settings: Settings | None = None,
) -> MicrostructureIngestionResult:
    settings = settings or get_settings()
    payloads: list[dict] = []
    batch_size = max(1, settings.microstructure_batch_size)

    for offset in range(0, len(asset_ids), batch_size):
        batch = asset_ids[offset : offset + batch_size]
        books = fetch_orderbooks(batch, settings)
        seen_assets = set()
        for book in books:
            asset_id = str(book.get("asset_id") or book.get("token_id") or "")
            if not asset_id:
                continue
            seen_assets.add(asset_id)
            payloads.append(
                {
                    "asset_id": asset_id,
                    "observed_at": book.get("timestamp"),
                    "payload": book,
                }
            )

        missing_assets = [asset_id for asset_id in batch if asset_id not in seen_assets]
        for asset_id in missing_assets:
            book = fetch_orderbook(asset_id, settings)
            if book is None:
                continue
            payloads.append(
                {
                    "asset_id": asset_id,
                    "observed_at": book.get("timestamp"),
                    "payload": book,
                }
            )

    inserted_count, duplicate_count = ingest_orderbook_payloads(payloads, settings)
    return MicrostructureIngestionResult(
        assets_requested=len(asset_ids),
        fetched_count=len(payloads),
        inserted_count=inserted_count,
        duplicate_count=duplicate_count,
    )


def ingest_orderbooks_for_wallet_assets(
    wallet: str,
    settings: Settings | None = None,
) -> MicrostructureIngestionResult:
    settings = settings or get_settings()
    with managed_connection(settings) as connection:
        asset_ids = [
            str(row["asset_id"])
            for row in connection.execute(
                """
                SELECT DISTINCT asset_id
                FROM trade_events
                WHERE wallet = ? AND asset_id IS NOT NULL
                ORDER BY asset_id
                """,
                (wallet,),
            ).fetchall()
        ]

    return ingest_orderbooks_for_assets(asset_ids, settings)
