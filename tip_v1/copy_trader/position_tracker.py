from __future__ import annotations

import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


logger = logging.getLogger(__name__)
EPS = Decimal("1e-9")
DEFAULT_TRADES_API = "https://data-api.polymarket.com/trades"
CACHE_TTL_SECONDS = 8.0  # re-fetch only if state older than this


@dataclass(frozen=True)
class MarketPosition:
    open_shares: float          # shares currently held (long inventory)
    open_cost_usdc: float       # cost basis of open inventory
    avg_buy_price: float        # weighted-avg buy price of open inventory
    realized_pnl_usdc: float    # cumulative realized PnL on this market
    this_trade_pnl_usdc: float  # PnL contribution of the most recent trade (sells only)
    trade_count: int            # total trades in this market


class PositionTracker:
    """Caches per-(wallet, condition_id) FIFO state. Re-fetches when stale."""

    def __init__(self, *, trades_api_url: str = DEFAULT_TRADES_API) -> None:
        self._cache: dict[tuple[str, str], tuple[float, MarketPosition]] = {}
        self._trades_api_url = trades_api_url

    def get_state(self, wallet: str, condition_id: str, asset: str) -> MarketPosition | None:
        """Compute (or return cached) position state for a wallet on a market."""
        key = (wallet.lower(), condition_id)
        cached = self._cache.get(key)
        now = time.time()
        if cached is not None and (now - cached[0]) < CACHE_TTL_SECONDS:
            return cached[1]
        try:
            state = self._compute_from_history(wallet, condition_id, asset)
        except Exception as exc:
            logger.warning("POSITION_FETCH_FAIL wallet=%s cid=%s reason=%s",
                           wallet[:10], condition_id[:14], exc)
            return None
        if state is not None:
            self._cache[key] = (now, state)
        return state

    def _compute_from_history(self, wallet: str, condition_id: str, asset: str) -> MarketPosition | None:
        trades = _fetch_user_trades(self._trades_api_url, wallet, limit=500)
        if not trades:
            return None

        # Filter to this condition + asset, sort by timestamp ascending
        legs = []
        for t in trades:
            if (t.get("conditionId") or "") != condition_id:
                continue
            if (t.get("asset") or "") != asset:
                continue
            side = (t.get("side") or "").upper()
            if side not in ("BUY", "SELL"):
                continue
            try:
                size = Decimal(str(t.get("size") or 0))
                price = Decimal(str(t.get("price") or 0))
            except Exception:
                continue
            if size <= 0:
                continue
            legs.append({
                "side": side, "size": size, "price": price,
                "ts": int(t.get("timestamp") or 0),
            })
        legs.sort(key=lambda l: l["ts"])

        open_buys: deque[dict[str, Decimal]] = deque()
        realized = Decimal(0)
        last_trade_pnl = Decimal(0)
        for leg in legs:
            if leg["side"] == "BUY":
                open_buys.append({"size": leg["size"], "price": leg["price"]})
                last_trade_pnl = Decimal(0)
                continue
            remaining = leg["size"]
            this_pnl = Decimal(0)
            while remaining > EPS and open_buys:
                lot = open_buys[0]
                matched = min(remaining, lot["size"])
                pnl = (leg["price"] - lot["price"]) * matched
                realized += pnl
                this_pnl += pnl
                lot["size"] -= matched
                remaining -= matched
                if lot["size"] <= EPS:
                    open_buys.popleft()
            last_trade_pnl = this_pnl

        open_shares = sum((lot["size"] for lot in open_buys), Decimal(0))
        open_cost = sum((lot["size"] * lot["price"] for lot in open_buys), Decimal(0))
        avg_buy = (open_cost / open_shares) if open_shares > 0 else Decimal(0)

        return MarketPosition(
            open_shares=float(open_shares),
            open_cost_usdc=float(open_cost),
            avg_buy_price=float(avg_buy),
            realized_pnl_usdc=float(realized),
            this_trade_pnl_usdc=float(last_trade_pnl),
            trade_count=len(legs),
        )


def _fetch_user_trades(api_url: str, wallet: str, *, limit: int = 500) -> list[dict[str, Any]]:
    url = f"{api_url}?{urlencode({'user': wallet, 'limit': limit, 'offset': 0})}"
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "tip-copy-trader/0.1"})
    with urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data if isinstance(data, list) else []
