from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.config import Settings, get_settings
from tip_v1.discovery.trade_scanner import fetch_recent_trades


@dataclass(frozen=True)
class MarketScannerWallet:
    wallet: str
    source: str
    regime: str
    source_rank: int | None
    source_metadata: dict[str, Any]


def _request_json(url: str, timeout_seconds: float) -> Any:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1-discovery/0.1",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_markets(
    *,
    settings: Settings | None = None,
    category: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    params = {
        "limit": limit,
        "closed": "false",
        "category": category,
    }
    payload = _request_json(
        f"{settings.markets_api_url}?{urlencode(params)}",
        settings.request_timeout_seconds,
    )
    if not isinstance(payload, list):
        raise ValueError("Markets API returned a non-list payload.")
    return [row for row in payload if isinstance(row, dict)]


def _market_matches_bucket(market: dict[str, Any], time_bucket: str) -> bool:
    title = str(market.get("question") or market.get("title") or "").lower()
    slug = str(market.get("slug") or "").lower()
    token = time_bucket.lower()
    return token in title or token in slug


def _extract_wallet(trade: dict[str, Any]) -> str | None:
    for field in ("maker", "taker", "user", "wallet"):
        wallet = trade.get(field)
        if isinstance(wallet, str) and wallet.startswith("0x"):
            return wallet.lower()
    return None


def scan_market_regime(
    *,
    settings: Settings | None = None,
    category: str,
    time_bucket: str,
    market_limit: int = 20,
    trade_limit: int = 100,
) -> list[MarketScannerWallet]:
    settings = settings or get_settings()
    regime = f"{category}_{time_bucket}"
    wallets: dict[tuple[str, str], MarketScannerWallet] = {}

    for market in fetch_markets(settings=settings, category=category, limit=market_limit):
        if not _market_matches_bucket(market, time_bucket):
            continue

        condition_id = (
            market.get("conditionId")
            or market.get("condition_id")
            or market.get("id")
        )
        if not isinstance(condition_id, str) or not condition_id:
            continue

        trades = fetch_recent_trades(
            settings=settings,
            limit=trade_limit,
            extra_params={"market": condition_id},
        )
        for trade in trades:
            wallet = _extract_wallet(trade)
            if not wallet:
                continue

            key = (wallet, condition_id)
            wallets[key] = MarketScannerWallet(
                wallet=wallet,
                source="market_scanner",
                regime=regime,
                source_rank=None,
                source_metadata={
                    "category": category,
                    "time_bucket": time_bucket,
                    "market_id": condition_id,
                    "market_title": market.get("question") or market.get("title"),
                },
            )

    return list(wallets.values())
