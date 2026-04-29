from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.copy_trader.position_tracker import PositionTracker
from tip_v1.copy_trader.telegram_alerter import TelegramAlerter
from tip_v1.copy_trader.watchlist import WatchedWallet, get_watchlist


logger = logging.getLogger(__name__)

DEFAULT_TRADES_API = "https://data-api.polymarket.com/trades"
DEFAULT_POLL_INTERVAL = 1.5  # seconds per wallet poll cycle
DEFAULT_DEDUPE_DB = Path(__file__).resolve().parent.parent / "tip_v1.sqlite3"


@dataclass(frozen=True)
class CopyTraderConfig:
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL
    trades_api_url: str = DEFAULT_TRADES_API
    fetch_limit: int = 20
    dry_run: bool = True
    enable_execution: bool = False  # if True, build ExecutionIntent and call PolymarketExecutor
    only_buys: bool = False         # phase-1: only alert on opens
    min_notional_usdc: float = 25.0 # ignore dust trades
    event_prefixes: tuple[str, ...] = ("cricipl-",)  # only alert on slugs starting with these

    @classmethod
    def from_env(cls) -> "CopyTraderConfig":
        prefixes_raw = os.getenv("COPY_TRADER_EVENT_PREFIXES", "cricipl-").strip()
        prefixes = tuple(p.strip() for p in prefixes_raw.split(",") if p.strip()) if prefixes_raw else ()
        return cls(
            poll_interval_seconds=float(os.getenv("COPY_TRADER_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)),
            trades_api_url=os.getenv("TIP_TRADES_API_URL", DEFAULT_TRADES_API),
            fetch_limit=int(os.getenv("COPY_TRADER_FETCH_LIMIT", "20")),
            dry_run=os.getenv("COPY_TRADER_DRY_RUN", "true").lower() != "false",
            enable_execution=os.getenv("COPY_TRADER_ENABLE_EXECUTION", "false").lower() == "true",
            only_buys=os.getenv("COPY_TRADER_ONLY_BUYS", "false").lower() == "true",
            min_notional_usdc=float(os.getenv("COPY_TRADER_MIN_NOTIONAL", "25")),
            event_prefixes=prefixes,
        )


def _ensure_dedupe_table(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_trader_seen (
                tx_hash TEXT PRIMARY KEY,
                wallet TEXT NOT NULL,
                seen_at INTEGER NOT NULL,
                payload TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_copy_trader_seen_wallet_ts "
            "ON copy_trader_seen(wallet, seen_at)"
        )
        conn.commit()


def _is_seen(db_path: Path, tx_hash: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM copy_trader_seen WHERE tx_hash = ?", (tx_hash,)
        ).fetchone()
    return row is not None


def _mark_seen(db_path: Path, tx_hash: str, wallet: str, payload: dict) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO copy_trader_seen (tx_hash, wallet, seen_at, payload) VALUES (?, ?, ?, ?)",
            (tx_hash, wallet, int(time.time()), json.dumps(payload, separators=(",", ":"))),
        )
        conn.commit()


def _fetch_recent_trades(api_url: str, wallet: str, limit: int) -> list[dict[str, Any]]:
    url = f"{api_url}?{urlencode({'user': wallet, 'limit': limit, 'offset': 0})}"
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "tip-copy-trader/0.1"})
    try:
        with urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as exc:
        logger.warning("FETCH_FAIL wallet=%s reason=%s", wallet[:10], exc)
        return []
    return data if isinstance(data, list) else []


def _build_execution_intent(trade: dict[str, Any], watched: WatchedWallet, signal_id: int):
    """Construct an ExecutionIntent for the existing PolymarketExecutor.
    Sized at watched.size_pct of source size, capped by max_per_trade_usdc."""
    from tip_v1.execution.executor import ExecutionIntent  # lazy import

    source_size = float(trade.get("size") or 0)
    price = float(trade.get("price") or 0)
    target_size = source_size * watched.size_pct
    if price > 0:
        target_notional = target_size * price
        if target_notional > watched.max_per_trade_usdc:
            target_size = watched.max_per_trade_usdc / price

    return ExecutionIntent(
        signal_id=signal_id,
        trigger_wallet=watched.address,
        market_id=str(trade.get("conditionId") or ""),
        asset_id=str(trade.get("asset") or ""),
        side=str(trade.get("side") or "").upper(),
        signal_timestamp=int(trade.get("timestamp") or time.time()),
        observed_trade_price=price,
        amount=target_size,
    )


def _process_trade(
    trade: dict[str, Any],
    watched: WatchedWallet,
    *,
    config: CopyTraderConfig,
    alerter: TelegramAlerter,
    executor: Any | None,
    db_path: Path,
    tracker: PositionTracker | None = None,
) -> None:
    tx = str(trade.get("transactionHash") or "")
    if not tx or _is_seen(db_path, tx):
        return

    side = str(trade.get("side") or "").upper()
    size = float(trade.get("size") or 0)
    price = float(trade.get("price") or 0)
    notional = size * price

    if config.only_buys and side != "BUY":
        _mark_seen(db_path, tx, watched.address, trade)
        return
    if notional < config.min_notional_usdc:
        _mark_seen(db_path, tx, watched.address, trade)
        return
    slug = str(trade.get("slug") or trade.get("eventSlug") or "")
    if config.event_prefixes and not any(slug.startswith(p) for p in config.event_prefixes):
        _mark_seen(db_path, tx, watched.address, trade)
        return

    position_dict: dict | None = None
    if tracker is not None:
        cid = str(trade.get("conditionId") or "")
        asset = str(trade.get("asset") or "")
        if cid and asset:
            ms = tracker.get_state(watched.address, cid, asset)
            if ms is not None:
                position_dict = {
                    "open_shares": ms.open_shares,
                    "open_cost_usdc": ms.open_cost_usdc,
                    "avg_buy_price": ms.avg_buy_price,
                    "realized_pnl_usdc": ms.realized_pnl_usdc,
                    "this_trade_pnl_usdc": ms.this_trade_pnl_usdc,
                    "trade_count": ms.trade_count,
                }
    watched_dict = {
        "pseudonym": watched.pseudonym,
        "lifetime_pnl_usdc": watched.lifetime_pnl_usdc,
        "lifetime_win_rate": watched.lifetime_win_rate,
        "lifetime_matches": watched.lifetime_matches,
    }
    text, callback_data = alerter.format_trade_alert(trade, watched_dict, position=position_dict)
    alerter.send(text, copy_callback_data=callback_data)
    logger.info(
        "WATCH_HIT wallet=%s side=%s size=%.0f price=%.4f notional=$%.0f slug=%s",
        watched.pseudonym, side, size, price, notional, trade.get("slug", "")[:40],
    )

    if config.enable_execution and executor is not None:
        signal_id = int(trade.get("timestamp") or time.time())
        intent = _build_execution_intent(trade, watched, signal_id)
        try:
            result = executor.execute(intent)
            logger.info(
                "EXEC_RESULT wallet=%s mode=%s status=%s order_id=%s error=%s",
                watched.pseudonym, result.mode, result.status, result.order_id, result.error,
            )
        except Exception as exc:
            logger.exception("EXEC_ERROR wallet=%s reason=%s", watched.pseudonym, exc)

    _mark_seen(db_path, tx, watched.address, trade)


def run_copy_trader(
    *,
    config: CopyTraderConfig | None = None,
    db_path: Path | None = None,
    max_cycles: int | None = None,
) -> None:
    config = config or CopyTraderConfig.from_env()
    db_path = db_path or Path(os.getenv("TIP_DB_PATH", DEFAULT_DEDUPE_DB))
    _ensure_dedupe_table(db_path)

    watchlist = get_watchlist()
    if not watchlist:
        logger.error("WATCHLIST_EMPTY — set COPY_TRADER_WATCHLIST or use the bundled defaults")
        return

    alerter = TelegramAlerter()
    tracker = PositionTracker(trades_api_url=config.trades_api_url)
    executor = None
    if config.enable_execution:
        from tip_v1.execution.executor import PolymarketExecutor  # lazy import
        executor = PolymarketExecutor(enabled=True, dry_run=config.dry_run)

    logger.info(
        "COPY_TRADER_START wallets=%d poll=%.2fs dry_run=%s execution=%s only_buys=%s min_notional=$%.0f prefixes=%s",
        len(watchlist), config.poll_interval_seconds, config.dry_run,
        config.enable_execution, config.only_buys, config.min_notional_usdc,
        ",".join(config.event_prefixes) or "<all>",
    )
    for w in watchlist.values():
        logger.info("  watching %s (%s) size_pct=%.2f cap=$%.0f",
                    w.pseudonym, w.address[:10], w.size_pct, w.max_per_trade_usdc)

    cycle = 0
    while True:
        for wallet, watched in watchlist.items():
            try:
                trades = _fetch_recent_trades(config.trades_api_url, wallet, config.fetch_limit)
            except Exception as exc:
                logger.exception("CYCLE_ERROR wallet=%s reason=%s", wallet[:10], exc)
                continue
            # process oldest-first so alerts arrive in trade order
            for t in sorted(trades, key=lambda x: int(x.get("timestamp") or 0)):
                _process_trade(
                    t, watched,
                    config=config, alerter=alerter, executor=executor,
                    db_path=db_path, tracker=tracker,
                )
            time.sleep(0.1)  # gentle pacing between wallet polls

        cycle += 1
        if max_cycles is not None and cycle >= max_cycles:
            return
        time.sleep(config.poll_interval_seconds)
