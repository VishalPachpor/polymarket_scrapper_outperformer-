from __future__ import annotations

import math
from collections import defaultdict
from typing import Any


def compute_sample_stats(trades: list[dict[str, Any]]) -> dict[str, int]:
    buy_count = 0
    sell_count = 0
    markets: set[str] = set()
    market_sides: defaultdict[str, set[str]] = defaultdict(set)

    for trade in trades:
        side = str(trade.get("side") or trade.get("action") or "").upper()
        market = trade.get("conditionId") or trade.get("market") or trade.get("marketId")

        if side == "BUY":
            buy_count += 1
        elif side == "SELL":
            sell_count += 1

        if isinstance(market, str) and market:
            markets.add(market)
            if side in {"BUY", "SELL"}:
                market_sides[market].add(side)

    two_sided_markets = sum(1 for sides in market_sides.values() if len(sides) > 1)

    return {
        "trade_count": len(trades),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "unique_markets": len(markets),
        "two_sided_markets": two_sided_markets,
    }


def compute_prefilter_score(
    stats: dict[str, Any],
    *,
    multi_source: bool = False,
) -> float:
    if int(stats.get("trade_count", 0)) == 0:
        return 0.0

    buy_count = int(stats.get("buy_count", 0))
    sell_count = int(stats.get("sell_count", 0))
    unique_markets = int(stats.get("unique_markets", 0))
    two_sided_markets = int(stats.get("two_sided_markets", 0))

    score = 0.0
    if sell_count > 0:
        score += 2.0
    if buy_count > 0:
        score += min(sell_count / buy_count, 1.0)
    score += math.log1p(unique_markets)
    if two_sided_markets > 0:
        score += 1.0
    if multi_source:
        score += 1.0
    return round(score, 4)


def passes_prefilter(stats: dict[str, Any]) -> tuple[bool, str]:
    if int(stats.get("sell_count", 0)) == 0:
        return False, "no_sells"
    if int(stats.get("trade_count", 0)) < 20:
        return False, "too_few_trades"
    return True, "ok"
