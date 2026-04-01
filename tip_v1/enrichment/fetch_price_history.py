from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class PriceHistoryFetchResult:
    wallet: str
    assets_requested: int
    assets_fetched: int
    assets_failed: int
    fetched_samples: int
    inserted_samples: int


def _request_json(url: str, *, timeout_seconds: float) -> dict:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1/0.1",
        },
    )

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except (URLError, OSError):
        # Fall back to curl when urllib networking is unavailable but shell HTTP still works.
        result = subprocess.run(
            ["curl", "-fsSL", url],
            check=True,
            capture_output=True,
            text=True,
            timeout=max(1.0, timeout_seconds),
        )
        return json.loads(result.stdout)


def _parse_history_payload(payload: object) -> list[dict]:
    if isinstance(payload, dict):
        history = payload.get("history")
        if isinstance(history, list):
            return history
        data = payload.get("data")
        if isinstance(data, dict):
            nested_history = data.get("history")
            if isinstance(nested_history, list):
                return nested_history
        if isinstance(data, list):
            return data
    elif isinstance(payload, list):
        return payload

    raise ValueError("Price history API returned a payload without a history list.")


def fetch_price_history_window(
    asset_id: str,
    *,
    start_ts: int,
    end_ts: int,
    fidelity: int,
    settings: Settings,
) -> list[dict]:
    params = {
        "market": asset_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity,
    }
    payload = _request_json(
        f"{settings.price_history_api_url}?{urlencode(params)}",
        timeout_seconds=settings.request_timeout_seconds,
    )
    return _parse_history_payload(payload)


def fetch_wallet_price_history(
    wallet: str,
    settings: Settings | None = None,
    *,
    fidelity: int | None = None,
    pre_buffer_seconds: int | None = None,
    post_buffer_seconds: int | None = None,
) -> PriceHistoryFetchResult:
    settings = settings or get_settings()
    fidelity = fidelity or settings.price_history_fidelity
    pre_buffer_seconds = (
        settings.price_history_pre_buffer_seconds
        if pre_buffer_seconds is None
        else pre_buffer_seconds
    )
    post_buffer_seconds = (
        settings.price_history_post_buffer_seconds
        if post_buffer_seconds is None
        else post_buffer_seconds
    )

    with managed_connection(settings) as connection:
        asset_windows = connection.execute(
            """
            SELECT
                asset_id,
                MIN(timestamp) AS min_timestamp,
                MAX(timestamp) AS max_timestamp
            FROM trade_events
            WHERE wallet = ? AND asset_id IS NOT NULL
            GROUP BY asset_id
            ORDER BY asset_id
            """,
            (wallet,),
        ).fetchall()

    assets_requested = len(asset_windows)
    assets_fetched = 0
    assets_failed = 0
    fetched_samples = 0
    inserted_samples = 0

    with managed_connection(settings) as connection:
        for window in asset_windows:
            asset_id = str(window["asset_id"])
            start_ts = int(window["min_timestamp"]) - pre_buffer_seconds
            end_ts = int(window["max_timestamp"]) + post_buffer_seconds
            try:
                history = fetch_price_history_window(
                    asset_id,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    fidelity=fidelity,
                    settings=settings,
                )
            except Exception:
                assets_failed += 1
                continue
            assets_fetched += 1
            fetched_samples += len(history)

            for sample in history:
                timestamp = int(sample["t"])
                price = float(sample["p"])
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO market_price_history (asset_id, timestamp, price)
                    VALUES (?, ?, ?)
                    """,
                    (asset_id, timestamp, price),
                )
                inserted_samples += cursor.rowcount

        connection.commit()

    return PriceHistoryFetchResult(
        wallet=wallet,
        assets_requested=assets_requested,
        assets_fetched=assets_fetched,
        assets_failed=assets_failed,
        fetched_samples=fetched_samples,
        inserted_samples=inserted_samples,
    )
