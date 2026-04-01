from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from tip_v1.wallet_engine.trade_stream import Trade


@dataclass(frozen=True)
class WalletStateConfig:
    recent_trade_window_seconds: int = 30
    active_market_window_seconds: int = 300
    max_recent_trades: int = 20


@dataclass(frozen=True)
class WalletActivityEvent:
    wallet: str
    market_id: str
    asset_id: str | None
    side: str
    price: float
    size: float
    timestamp: int
    burst_score: int
    trades_in_window: int
    same_market_trades: int
    active_market_count: int
    last_trade_ts: int


@dataclass
class WalletLiveState:
    wallet: str
    recent_trades: deque[Trade] = field(default_factory=deque)
    last_trade_ts: int | None = None


class WalletStateTracker:
    def __init__(self, *, config: WalletStateConfig | None = None) -> None:
        self.config = config or WalletStateConfig()
        self._states: dict[str, WalletLiveState] = {}

    def update(self, trade: Trade) -> WalletActivityEvent:
        state = self._states.setdefault(
            trade.wallet,
            WalletLiveState(
                wallet=trade.wallet,
                recent_trades=deque(maxlen=self.config.max_recent_trades),
            ),
        )
        state.recent_trades.append(trade)
        state.last_trade_ts = trade.timestamp
        self._prune_stale(state, now_ts=trade.timestamp)

        trades_in_window = sum(
            1
            for item in state.recent_trades
            if trade.timestamp - item.timestamp <= self.config.recent_trade_window_seconds
        )
        same_market_trades = sum(
            1
            for item in state.recent_trades
            if item.market_id == trade.market_id
            and trade.timestamp - item.timestamp <= self.config.recent_trade_window_seconds
        )
        active_markets = {
            item.market_id
            for item in state.recent_trades
            if trade.timestamp - item.timestamp <= self.config.active_market_window_seconds
        }
        return WalletActivityEvent(
            wallet=trade.wallet,
            market_id=trade.market_id,
            asset_id=trade.asset_id,
            side=trade.side,
            price=trade.price,
            size=trade.size,
            timestamp=trade.timestamp,
            burst_score=max(0, trades_in_window - 1),
            trades_in_window=trades_in_window,
            same_market_trades=same_market_trades,
            active_market_count=len(active_markets),
            last_trade_ts=state.last_trade_ts or trade.timestamp,
        )

    def _prune_stale(self, state: WalletLiveState, *, now_ts: int) -> None:
        while state.recent_trades:
            oldest = state.recent_trades[0]
            if now_ts - oldest.timestamp <= self.config.active_market_window_seconds:
                break
            state.recent_trades.popleft()
