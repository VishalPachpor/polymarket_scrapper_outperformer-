from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"


@dataclass(frozen=True)
class LeaderboardWallet:
    wallet: str
    source: str
    regime: str
    source_rank: int | None
    source_metadata: dict[str, Any]


def _leaderboard_base_url() -> str:
    return os.getenv("TIP_LEADERBOARD_API_URL", DEFAULT_LEADERBOARD_URL)


def _request_json(url: str) -> Any:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1-discovery/0.1",
        },
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if isinstance(payload, dict):
        for key in ("data", "rows", "results", "leaderboard"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]

    raise ValueError("Leaderboard API returned an unsupported payload shape.")


def fetch_leaderboard_wallets(
    *,
    category: str = "crypto",
    interval: str = "monthly",
    metric: str = "profit",
    limit: int = 50,
) -> list[LeaderboardWallet]:
    interval_map = {
        "daily": "DAY",
        "weekly": "WEEK",
        "monthly": "MONTH",
        "all": "ALL",
    }
    params = {
        "category": category.upper(),
        "timePeriod": interval_map.get(interval.lower(), interval.upper()),
        "orderBy": "VOL" if metric.lower() == "volume" else "PNL",
        "limit": limit,
    }
    payload = _request_json(f"{_leaderboard_base_url()}?{urlencode(params)}")
    rows = _extract_rows(payload)

    wallets: list[LeaderboardWallet] = []
    source = f"leaderboard_{interval}_{metric}"
    for index, row in enumerate(rows[:limit], start=1):
        wallet = row.get("proxyWallet") or row.get("address") or row.get("wallet")
        if not wallet or not isinstance(wallet, str):
            continue
        wallets.append(
            LeaderboardWallet(
                wallet=wallet.lower(),
                source=source,
                regime="global",
                source_rank=int(row.get("rank") or index),
                source_metadata={
                    "category": category,
                    "interval": interval,
                    "metric": metric,
                    "profit": row.get("pnl") if row.get("pnl") is not None else row.get("profit"),
                    "volume": row.get("vol") if row.get("vol") is not None else row.get("volume"),
                    "name": row.get("userName") or row.get("name") or row.get("username"),
                },
            )
        )
    return wallets


def fetch_default_leaderboard_wallets(limit: int = 50) -> list[LeaderboardWallet]:
    wallets: list[LeaderboardWallet] = []
    seen: set[tuple[str, str]] = set()
    for interval, metric in (
        ("daily", "profit"),
        ("monthly", "profit"),
        ("monthly", "volume"),
    ):
        for row in fetch_leaderboard_wallets(
            category="crypto",
            interval=interval,
            metric=metric,
            limit=limit,
        ):
            key = (row.wallet, row.source)
            if key in seen:
                continue
            seen.add(key)
            wallets.append(row)
    return wallets
