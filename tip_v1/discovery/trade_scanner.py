from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.config import Settings, get_settings


@dataclass(frozen=True)
class TradeStreamWallet:
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


def fetch_recent_trades(
    *,
    settings: Settings | None = None,
    limit: int = 200,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    params = {"limit": limit}
    if extra_params:
        params.update(extra_params)
    payload = _request_json(
        f"{settings.trades_api_url}?{urlencode(params)}",
        settings.request_timeout_seconds,
    )
    if not isinstance(payload, list):
        raise ValueError("Recent trades API returned a non-list payload.")
    return [row for row in payload if isinstance(row, dict)]


def fetch_wallet_trades(
    wallet: str,
    *,
    settings: Settings | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    params = {
        "user": wallet,
        "limit": limit,
        "offset": 0,
        "takerOnly": "true",
    }
    payload = _request_json(
        f"{settings.trades_api_url}?{urlencode(params)}",
        settings.request_timeout_seconds,
    )
    if not isinstance(payload, list):
        raise ValueError("Wallet trades API returned a non-list payload.")
    return [row for row in payload if isinstance(row, dict)]


def extract_wallets_from_recent_trades(
    trades: list[dict[str, Any]],
    *,
    min_trade_count: int = 5,
) -> list[TradeStreamWallet]:
    counts: Counter[str] = Counter()
    for trade in trades:
        for field in ("user", "maker", "taker", "wallet", "proxyWallet"):
            wallet = trade.get(field)
            if isinstance(wallet, str) and wallet.startswith("0x"):
                counts[wallet.lower()] += 1
                break

    ranked = counts.most_common()
    wallets: list[TradeStreamWallet] = []
    for rank, (wallet, trade_count) in enumerate(ranked, start=1):
        if trade_count < min_trade_count:
            continue
        wallets.append(
            TradeStreamWallet(
                wallet=wallet,
                source="trade_stream",
                regime="global",
                source_rank=rank,
                source_metadata={"trade_count": trade_count},
            )
        )
    return wallets
