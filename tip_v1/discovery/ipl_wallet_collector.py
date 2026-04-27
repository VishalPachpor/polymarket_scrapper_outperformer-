from __future__ import annotations

import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.config import Settings, get_settings
from tip_v1.discovery.ipl_market_scanner import IPLMarket


EPSILON = Decimal("1e-9")

# Polymarket data-api rejects offsets > 3000 with HTTP 400.
TRADES_API_MAX_OFFSET = 3000


@dataclass
class WalletLeg:
    asset: str
    condition_id: str
    event_slug: str
    question: str
    outcome: str
    side: str
    size: Decimal
    price: Decimal
    timestamp: int


@dataclass
class WalletIPLStats:
    wallet: str
    pseudonym: str = ""
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    matched_buy_cost: Decimal = Decimal("0")
    matched_sell_value: Decimal = Decimal("0")
    open_inventory_cost: Decimal = Decimal("0")
    open_inventory_value: Decimal = Decimal("0")
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    closed_position_count: int = 0
    winning_position_count: int = 0
    total_volume_usdc: Decimal = Decimal("0")
    markets_traded: set[str] = field(default_factory=set)
    first_trade_ts: int | None = None
    last_trade_ts: int | None = None


def _request_json(url: str, timeout_seconds: float, *, retries: int = 3) -> Any:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1-discovery/0.1",
        },
    )
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            # Do not retry HTTP 4xx (e.g. offset-cap rejections) — only transient errors.
            status = getattr(exc, "code", None)
            if isinstance(status, int) and 400 <= status < 500:
                raise
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(0.5 * (2 ** attempt))
    assert last_exc is not None
    raise last_exc


def fetch_all_market_trades(
    condition_id: str,
    *,
    settings: Settings | None = None,
    page_limit: int = 500,
    max_pages: int = 200,
    sleep_between_pages: float = 0.05,
    debug: bool = False,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    all_trades: list[dict[str, Any]] = []
    offset = 0
    truncated = False
    for _ in range(max_pages):
        if offset > TRADES_API_MAX_OFFSET:
            truncated = True
            break
        effective_limit = min(page_limit, TRADES_API_MAX_OFFSET - offset + page_limit)
        params = {
            "market": condition_id,
            "limit": effective_limit,
            "offset": offset,
        }
        url = f"{settings.trades_api_url}?{urlencode(params)}"
        if debug:
            print(f"   GET {url}", flush=True)
        try:
            payload = _request_json(url, settings.request_timeout_seconds)
        except Exception as exc:
            body = ""
            if hasattr(exc, "read"):
                try:
                    body = exc.read().decode("utf-8", errors="replace")[:300]  # type: ignore[attr-defined]
                except Exception:
                    body = "<unreadable body>"
            raise RuntimeError(f"trades API request failed: {exc} body={body!r}") from exc
        if not isinstance(payload, list) or not payload:
            break
        all_trades.extend(t for t in payload if isinstance(t, dict))
        if len(payload) < effective_limit:
            break
        offset += effective_limit
        if sleep_between_pages > 0:
            time.sleep(sleep_between_pages)
    if truncated and debug:
        print(f"   note: hit API offset cap ({TRADES_API_MAX_OFFSET}); trade history truncated", flush=True)
    return all_trades


def _outcome_for_index(market: IPLMarket, outcome_index: int | None) -> str:
    if outcome_index is None:
        return ""
    if 0 <= outcome_index < len(market.outcomes):
        return market.outcomes[outcome_index]
    return ""


def _resolution_price(market: IPLMarket, outcome_index: int | None) -> Decimal | None:
    if not market.closed or outcome_index is None:
        return None
    if 0 <= outcome_index < len(market.outcome_prices):
        return Decimal(str(market.outcome_prices[outcome_index]))
    return None


def collect_wallet_legs(
    market: IPLMarket,
    trades: list[dict[str, Any]],
) -> dict[str, list[WalletLeg]]:
    legs_by_wallet: dict[str, list[WalletLeg]] = defaultdict(list)
    for trade in trades:
        wallet = trade.get("proxyWallet")
        side = trade.get("side")
        asset = trade.get("asset")
        if not isinstance(wallet, str) or not isinstance(side, str) or not isinstance(asset, str):
            continue
        try:
            size = Decimal(str(trade.get("size") or 0))
            price = Decimal(str(trade.get("price") or 0))
        except Exception:
            continue
        if size <= 0 or price < 0:
            continue
        timestamp = int(trade.get("timestamp") or 0)
        outcome_index = trade.get("outcomeIndex")
        outcome = _outcome_for_index(
            market,
            int(outcome_index) if isinstance(outcome_index, (int, float)) else None,
        )
        legs_by_wallet[wallet.lower()].append(
            WalletLeg(
                asset=asset,
                condition_id=market.condition_id,
                event_slug=market.event_slug,
                question=market.question,
                outcome=outcome,
                side=side.upper(),
                size=size,
                price=price,
                timestamp=timestamp,
            )
        )
    return legs_by_wallet


def _last_trade_price_per_asset(trades: list[dict[str, Any]]) -> dict[str, Decimal]:
    last_prices: dict[str, tuple[int, Decimal]] = {}
    for trade in trades:
        asset = trade.get("asset")
        if not isinstance(asset, str):
            continue
        try:
            price = Decimal(str(trade.get("price") or 0))
        except Exception:
            continue
        timestamp = int(trade.get("timestamp") or 0)
        prior = last_prices.get(asset)
        if prior is None or timestamp >= prior[0]:
            last_prices[asset] = (timestamp, price)
    return {asset: price for asset, (_ts, price) in last_prices.items()}


def _outcome_index_for_asset(market: IPLMarket, asset: str) -> int | None:
    if asset in market.clob_token_ids:
        return market.clob_token_ids.index(asset)
    return None


def update_wallet_stats(
    stats: WalletIPLStats,
    legs: list[WalletLeg],
    market: IPLMarket,
    last_trade_prices: dict[str, Decimal],
) -> None:
    legs_by_asset: dict[str, list[WalletLeg]] = defaultdict(list)
    for leg in legs:
        legs_by_asset[leg.asset].append(leg)

    for asset, asset_legs in legs_by_asset.items():
        asset_legs.sort(key=lambda l: l.timestamp)
        open_buys: deque[dict[str, Any]] = deque()

        for leg in asset_legs:
            stats.trade_count += 1
            stats.total_volume_usdc += leg.size * leg.price
            stats.markets_traded.add(market.condition_id)
            if stats.first_trade_ts is None or leg.timestamp < stats.first_trade_ts:
                stats.first_trade_ts = leg.timestamp
            if stats.last_trade_ts is None or leg.timestamp > stats.last_trade_ts:
                stats.last_trade_ts = leg.timestamp

            if leg.side == "BUY":
                stats.buy_count += 1
                open_buys.append({"price": leg.price, "remaining_size": leg.size})
                continue

            stats.sell_count += 1
            remaining = leg.size
            while remaining > EPSILON and open_buys:
                lot = open_buys[0]
                matched = min(remaining, lot["remaining_size"])
                pnl = (leg.price - lot["price"]) * matched
                stats.matched_buy_cost += lot["price"] * matched
                stats.matched_sell_value += leg.price * matched
                stats.realized_pnl += pnl
                stats.closed_position_count += 1
                if pnl > 0:
                    stats.winning_position_count += 1
                lot["remaining_size"] -= matched
                remaining -= matched
                if lot["remaining_size"] <= EPSILON:
                    open_buys.popleft()

        if not open_buys:
            continue

        outcome_index = _outcome_index_for_asset(market, asset)
        resolution = _resolution_price(market, outcome_index)
        mark_price = resolution if resolution is not None else last_trade_prices.get(asset, Decimal("0"))

        for lot in open_buys:
            cost = lot["price"] * lot["remaining_size"]
            value = mark_price * lot["remaining_size"]
            stats.open_inventory_cost += cost
            stats.open_inventory_value += value
            stats.unrealized_pnl += value - cost


def aggregate_wallet_stats(
    markets: list[IPLMarket],
    *,
    settings: Settings | None = None,
    progress: bool = True,
) -> dict[str, WalletIPLStats]:
    settings = settings or get_settings()
    wallet_stats: dict[str, WalletIPLStats] = {}
    pseudonyms: dict[str, str] = {}

    for index, market in enumerate(markets, start=1):
        if progress:
            print(
                f"[{index}/{len(markets)}] {market.event_slug} :: {market.question[:60]}",
                flush=True,
            )
        try:
            trades = fetch_all_market_trades(market.condition_id, settings=settings)
        except Exception as exc:
            if progress:
                print(f"  ! fetch failed: {exc}", flush=True)
            continue

        for trade in trades:
            wallet = trade.get("proxyWallet")
            pseudonym = trade.get("pseudonym")
            if isinstance(wallet, str) and isinstance(pseudonym, str) and pseudonym:
                pseudonyms.setdefault(wallet.lower(), pseudonym)

        legs_by_wallet = collect_wallet_legs(market, trades)
        last_trade_prices = _last_trade_price_per_asset(trades)

        for wallet, legs in legs_by_wallet.items():
            stats = wallet_stats.get(wallet)
            if stats is None:
                stats = WalletIPLStats(wallet=wallet)
                wallet_stats[wallet] = stats
            update_wallet_stats(stats, legs, market, last_trade_prices)

        if progress:
            print(
                f"  trades={len(trades)} new_wallets={len(legs_by_wallet)} total_wallets={len(wallet_stats)}",
                flush=True,
            )

    for wallet, stats in wallet_stats.items():
        stats.pseudonym = pseudonyms.get(wallet, "")

    return wallet_stats
