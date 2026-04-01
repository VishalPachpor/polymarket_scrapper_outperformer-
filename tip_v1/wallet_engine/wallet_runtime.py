from __future__ import annotations

import argparse
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database
from tip_v1.wallet_engine.signal_builder import (
    WalletSignal,
    WalletSignalDecision,
    WalletSignalConfig,
    evaluate_wallet_signal,
)
from tip_v1.wallet_engine.trade_stream import DbBackedTradeStream, Trade, TradeStreamConfig
from tip_v1.wallet_engine.wallet_registry import WalletRegistry, WalletRegistryConfig
from tip_v1.wallet_engine.wallet_scoring import score_wallet_activity
from tip_v1.wallet_engine.wallet_state import WalletActivityEvent, WalletStateConfig, WalletStateTracker


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WalletRuntimeConfig:
    stream: TradeStreamConfig = TradeStreamConfig()
    registry: WalletRegistryConfig = WalletRegistryConfig()
    state: WalletStateConfig = WalletStateConfig()
    signal: WalletSignalConfig = WalletSignalConfig()
    market_context_window_seconds: int = 30
    emit_shadow_candidates: bool = False


@dataclass(frozen=True)
class WalletRuntimeCycle:
    trades_seen: int
    top_wallet_trades: int
    shadow_candidates: int
    signals_emitted: int
    registry_size: int
    last_event_id: int
    latency_seconds: float
    rejection_counts: dict[str, int]


class WalletEngineRuntime:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        config: WalletRuntimeConfig | None = None,
        on_signal_callback: Callable[[WalletSignal], None] | None = None,
        on_shadow_candidate_callback: Callable[[WalletSignal], None] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or WalletRuntimeConfig()
        self.on_signal_callback = on_signal_callback
        self.on_shadow_candidate_callback = on_shadow_candidate_callback
        self.state = WalletStateTracker(config=self.config.state)
        self.registry = WalletRegistry(settings=self.settings, config=self.config.registry)
        self.market_trades: dict[str, deque[Trade]] = defaultdict(deque)
        self.recent_top_activity: dict[str, deque[tuple[str, str, int]]] = defaultdict(deque)
        self._pending_signals: list[WalletSignal] = []
        self._stream_trades_seen = 0
        self._top_wallet_trades = 0
        self._shadow_candidates = 0
        self._rejection_counts: dict[str, int] = defaultdict(int)
        self.trade_stream = DbBackedTradeStream(
            settings=self.settings,
            config=self.config.stream,
            on_trade_callback=self._on_trade,
        )

    def run_once(self) -> WalletRuntimeCycle:
        now_ts = int(time.time())
        wallets = self.registry.refresh(now_ts=now_ts)
        self._pending_signals = []
        self._stream_trades_seen = 0
        self._top_wallet_trades = 0
        self._shadow_candidates = 0
        self._rejection_counts = defaultdict(int)
        cycle = self.trade_stream.run_once()
        for signal in self._pending_signals:
            if self.on_signal_callback is not None:
                self.on_signal_callback(signal)
            logger.info(
                "wallet_signal_generated wallet=%s market_id=%s side=%s price=%.4f score=%.3f confidence=%s aligned_wallets=%s",
                signal.wallet,
                signal.market_id,
                signal.side,
                signal.price,
                signal.score,
                signal.confidence,
                signal.aligned_wallet_count,
            )
        result = WalletRuntimeCycle(
            trades_seen=self._stream_trades_seen,
            top_wallet_trades=self._top_wallet_trades,
            shadow_candidates=self._shadow_candidates,
            signals_emitted=len(self._pending_signals),
            registry_size=len(wallets),
            last_event_id=cycle.last_event_id,
            latency_seconds=cycle.latency_seconds,
            rejection_counts=dict(self._rejection_counts),
        )
        logger.info(
            "wallet_runtime_cycle trades_seen=%s top_wallet_trades=%s shadow_candidates=%s signals_emitted=%s registry_size=%s last_event_id=%s latency=%.2fs rejection_counts=%s",
            result.trades_seen,
            result.top_wallet_trades,
            result.shadow_candidates,
            result.signals_emitted,
            result.registry_size,
            result.last_event_id,
            result.latency_seconds,
            dict(result.rejection_counts),
        )
        return result

    def run_forever(self, *, max_cycles: int | None = None) -> None:
        cycle_count = 0
        while True:
            try:
                self.run_once()
            except Exception as exc:
                logger.exception("wallet_runtime_error error=%s", exc)
            cycle_count += 1
            if max_cycles is not None and cycle_count >= max_cycles:
                return
            time.sleep(self.config.stream.poll_interval_seconds)

    def _on_trade(self, trade: Trade) -> None:
        self._stream_trades_seen += 1
        self._update_market_context(trade)
        base = self.registry.get(trade.wallet)
        if base is None:
            return
        self._top_wallet_trades += 1
        activity = self.state.update(trade)
        live_score = score_wallet_activity(base, activity)
        aligned_wallets = self._aligned_wallet_count(trade, activity)
        decision = evaluate_wallet_signal(
            trade=trade,
            activity=activity,
            wallet_score=live_score,
            recent_market_trades=self.market_trades[trade.market_id],
            aligned_wallet_count=aligned_wallets,
            config=self.config.signal,
        )
        self.recent_top_activity[trade.market_id].append((trade.wallet, trade.side, trade.timestamp))
        self._prune_top_activity(trade.market_id, now_ts=trade.timestamp)
        self._shadow_candidates += 1
        if self.config.emit_shadow_candidates and self.on_shadow_candidate_callback is not None:
            self.on_shadow_candidate_callback(decision.signal)
        if decision.accepted:
            self._pending_signals.append(decision.signal)
            return

        reason = decision.rejection_reason or "unknown_rejection"
        self._rejection_counts[reason] += 1
        logger.debug(
            "wallet_signal_rejected wallet=%s market_id=%s side=%s reason=%s price=%.4f score=%.3f aligned_wallets=%s",
            decision.signal.wallet,
            decision.signal.market_id,
            decision.signal.side,
            reason,
            decision.signal.price,
            decision.signal.score,
            decision.signal.aligned_wallet_count,
        )

    def _update_market_context(self, trade: Trade) -> None:
        trades = self.market_trades[trade.market_id]
        trades.append(trade)
        while trades and trade.timestamp - trades[0].timestamp > self.config.market_context_window_seconds:
            trades.popleft()

    def _aligned_wallet_count(self, trade: Trade, activity: WalletActivityEvent) -> int:
        activity_log = self.recent_top_activity[trade.market_id]
        self._prune_top_activity(trade.market_id, now_ts=trade.timestamp)
        wallets = {
            wallet
            for wallet, side, timestamp in activity_log
            if side == trade.side
            and trade.timestamp - timestamp <= self.config.signal.alignment_window_seconds
        }
        wallets.add(trade.wallet)
        return len(wallets)

    def _prune_top_activity(self, market_id: str, *, now_ts: int) -> None:
        activity_log = self.recent_top_activity[market_id]
        while activity_log and now_ts - activity_log[0][2] > self.config.signal.alignment_window_seconds:
            activity_log.popleft()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TIP V1 wallet tracking runtime.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--poll-interval", type=int, default=2, help="Wallet runtime polling interval in seconds.")
    parser.add_argument("--trade-limit", type=int, default=200, help="Recent trade batch size per poll.")
    parser.add_argument("--normalize-batch-size", type=int, default=500, help="Normalization batch size per cycle.")
    parser.add_argument("--top-wallets", type=int, default=100, help="Number of top wallets to keep in memory.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit.")
    parser.add_argument("--max-cycles", type=int, help="Optional maximum cycles for continuous mode.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = get_settings(args.db_path)
    initialize_database(settings)
    runtime = WalletEngineRuntime(
        settings=settings,
        config=WalletRuntimeConfig(
            stream=TradeStreamConfig(
                poll_interval_seconds=args.poll_interval,
                recent_trade_limit=args.trade_limit,
                normalize_batch_size=args.normalize_batch_size,
            ),
            registry=WalletRegistryConfig(top_n=args.top_wallets),
        ),
    )
    if args.once:
        runtime.run_once()
        return
    runtime.run_forever(max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()
