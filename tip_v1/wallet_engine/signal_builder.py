from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from tip_v1.wallet_engine.trade_stream import Trade
from tip_v1.wallet_engine.wallet_scoring import WalletLiveScore
from tip_v1.wallet_engine.wallet_state import WalletActivityEvent


@dataclass(frozen=True)
class WalletSignalConfig:
    min_live_score: float = 0.55
    min_burst_score: int = 1
    alignment_window_seconds: int = 20
    anti_chase_lookback_seconds: int = 5
    max_price_move_pct: float = 0.05
    require_specialist_market: bool = True


@dataclass(frozen=True)
class WalletSignal:
    market_id: str
    asset_id: str | None
    wallet: str
    side: str
    timestamp: int
    price: float
    score: float
    confidence: str
    aligned_wallet_count: int
    reason: str | None = None


@dataclass(frozen=True)
class WalletSignalDecision:
    signal: WalletSignal
    accepted: bool
    rejection_reason: str | None = None


def evaluate_wallet_signal(
    *,
    trade: Trade,
    activity: WalletActivityEvent,
    wallet_score: WalletLiveScore,
    recent_market_trades: deque[Trade],
    aligned_wallet_count: int,
    config: WalletSignalConfig | None = None,
) -> WalletSignalDecision:
    config = config or WalletSignalConfig()
    total_score = wallet_score.live_score + min(0.20, 0.05 * max(0, aligned_wallet_count - 1))
    if total_score >= 0.85:
        confidence = "HIGH"
    elif total_score >= 0.60:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"
    candidate = WalletSignal(
        market_id=trade.market_id,
        asset_id=trade.asset_id,
        wallet=trade.wallet,
        side=trade.side,
        timestamp=trade.timestamp,
        price=trade.price,
        score=total_score,
        confidence=confidence,
        aligned_wallet_count=aligned_wallet_count,
    )

    if wallet_score.live_score < config.min_live_score:
        return WalletSignalDecision(
            signal=WalletSignal(**{**candidate.__dict__, "reason": "low_live_score"}),
            accepted=False,
            rejection_reason="low_live_score",
        )
    if activity.burst_score < config.min_burst_score:
        return WalletSignalDecision(
            signal=WalletSignal(**{**candidate.__dict__, "reason": "no_burst"}),
            accepted=False,
            rejection_reason="no_burst",
        )
    if config.require_specialist_market and not wallet_score.market_specialist:
        return WalletSignalDecision(
            signal=WalletSignal(**{**candidate.__dict__, "reason": "market_not_specialized"}),
            accepted=False,
            rejection_reason="market_not_specialized",
        )
    if _is_chasing_move(
        trade=trade,
        recent_market_trades=recent_market_trades,
        lookback_seconds=config.anti_chase_lookback_seconds,
        max_price_move_pct=config.max_price_move_pct,
    ):
        return WalletSignalDecision(
            signal=WalletSignal(**{**candidate.__dict__, "reason": "chasing_move"}),
            accepted=False,
            rejection_reason="chasing_move",
        )

    return WalletSignalDecision(signal=candidate, accepted=True)


def build_wallet_signal(
    *,
    trade: Trade,
    activity: WalletActivityEvent,
    wallet_score: WalletLiveScore,
    recent_market_trades: deque[Trade],
    aligned_wallet_count: int,
    config: WalletSignalConfig | None = None,
) -> WalletSignal | None:
    decision = evaluate_wallet_signal(
        trade=trade,
        activity=activity,
        wallet_score=wallet_score,
        recent_market_trades=recent_market_trades,
        aligned_wallet_count=aligned_wallet_count,
        config=config,
    )
    return decision.signal if decision.accepted else None


def _is_chasing_move(
    *,
    trade: Trade,
    recent_market_trades: deque[Trade],
    lookback_seconds: int,
    max_price_move_pct: float,
) -> bool:
    baseline_price: float | None = None
    for prior_trade in reversed(recent_market_trades):
        if trade.timestamp - prior_trade.timestamp > lookback_seconds:
            break
        baseline_price = prior_trade.price
    if baseline_price is None or baseline_price <= 0:
        return False
    move_pct = abs((trade.price - baseline_price) / baseline_price)
    return move_pct > max_price_move_pct
