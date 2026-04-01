from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable, Sequence

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


BUY = "BUY"
NO_SIGNAL = "NO_SIGNAL"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MomentumTriggerConfig:
    min_entry_price: float = 0.30
    max_entry_price: float = 0.50
    lookback_seconds: int = 60
    min_price_change: float = 0.03
    max_price_change: float = 0.08
    min_consecutive_up_ticks: int = 3
    min_velocity_per_second: float = 0.0005
    min_samples: int = 4
    min_observation_span_seconds: int = 20
    max_signal_age_seconds: int = 15
    cooldown_seconds: int = 120


@dataclass(frozen=True)
class PriceSample:
    market_id: str
    asset_id: str
    timestamp: int
    price: float


@dataclass(frozen=True)
class MomentumSignal:
    market_id: str | None
    asset_id: str | None
    timestamp: int | None
    price: float | None
    signal_type: str
    confidence: float
    dry_run: bool = False
    reason: str | None = None


def load_recent_trade_samples(
    connection,
    *,
    asset_id: str | None = None,
    market_id: str | None = None,
    outcome: str | None = None,
    lookback_seconds: int = 60,
    now_ts: int | None = None,
    limit: int = 120,
) -> list[PriceSample]:
    if asset_id is None and market_id is None:
        raise ValueError("asset_id or market_id is required to load recent trade samples")

    clauses = [
        "asset_id IS NOT NULL",
        "price IS NOT NULL",
    ]
    params: list[object] = []

    if asset_id is not None:
        clauses.append("asset_id = ?")
        params.append(asset_id)
    if market_id is not None:
        clauses.append("market_id = ?")
        params.append(market_id)
    if outcome is not None:
        clauses.append("outcome = ?")
        params.append(outcome)
    if now_ts is not None:
        clauses.append("timestamp <= ?")
        params.append(now_ts)
        clauses.append("timestamp >= ?")
        params.append(now_ts - lookback_seconds)

    rows = connection.execute(
        f"""
        SELECT market_id, asset_id, timestamp, CAST(price AS REAL) AS price
        FROM trade_events
        WHERE {' AND '.join(clauses)}
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()

    ordered_rows = reversed(rows)
    return [
        PriceSample(
            market_id=str(row["market_id"]),
            asset_id=str(row["asset_id"]),
            timestamp=int(row["timestamp"]),
            price=float(row["price"]),
        )
        for row in ordered_rows
    ]


def _no_signal(
    *,
    sample: PriceSample | None,
    reason: str,
    dry_run: bool,
) -> MomentumSignal:
    return MomentumSignal(
        market_id=sample.market_id if sample is not None else None,
        asset_id=sample.asset_id if sample is not None else None,
        timestamp=sample.timestamp if sample is not None else None,
        price=sample.price if sample is not None else None,
        signal_type=NO_SIGNAL,
        confidence=0.0,
        dry_run=dry_run,
        reason=reason,
    )


def _log_signal(signal: MomentumSignal) -> MomentumSignal:
    if signal.signal_type == BUY:
        logger.info(
            "momentum_signal_generated market_id=%s asset_id=%s ts=%s price=%.4f confidence=%.3f dry_run=%s",
            signal.market_id,
            signal.asset_id,
            signal.timestamp,
            signal.price if signal.price is not None else -1.0,
            signal.confidence,
            signal.dry_run,
        )
    else:
        logger.info(
            "momentum_signal_rejected market_id=%s asset_id=%s ts=%s reason=%s dry_run=%s",
            signal.market_id,
            signal.asset_id,
            signal.timestamp,
            signal.reason,
            signal.dry_run,
        )
    return signal


def _count_consecutive_up_ticks(samples: Sequence[PriceSample]) -> int:
    count = 0
    for idx in range(len(samples) - 1, 0, -1):
        if samples[idx].price > samples[idx - 1].price:
            count += 1
            continue
        break
    return count


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _compute_confidence(
    latest: PriceSample,
    earliest: PriceSample,
    *,
    price_change: float,
    up_ticks: int,
    velocity: float,
    config: MomentumTriggerConfig,
) -> float:
    zone_midpoint = (config.min_entry_price + config.max_entry_price) / 2.0
    half_zone_width = max((config.max_entry_price - config.min_entry_price) / 2.0, 0.0001)
    zone_score = _clamp(1.0 - (abs(latest.price - zone_midpoint) / half_zone_width))
    move_score = _clamp(
        (price_change - config.min_price_change)
        / max(config.max_price_change - config.min_price_change, 0.0001)
    )
    tick_score = _clamp(up_ticks / max(config.min_consecutive_up_ticks + 1, 1))
    velocity_score = _clamp(velocity / max(config.min_velocity_per_second * 2.0, 0.0001))

    # Keep the score simple and explainable: move quality, tape consistency,
    # and whether price is still in the center of the target entry zone.
    return round(
        (0.35 * move_score) + (0.25 * tick_score) + (0.20 * velocity_score) + (0.20 * zone_score),
        4,
    )


def evaluate_momentum_signal(
    samples: Iterable[PriceSample],
    *,
    config: MomentumTriggerConfig | None = None,
    now_ts: int | None = None,
    last_signal_timestamp: int | None = None,
    dry_run: bool = False,
) -> MomentumSignal:
    config = config or MomentumTriggerConfig()
    ordered_samples = sorted(samples, key=lambda sample: (sample.timestamp, sample.price))

    if not ordered_samples:
        return _log_signal(_no_signal(sample=None, reason="no_samples", dry_run=dry_run))

    latest = ordered_samples[-1]
    now_ts = latest.timestamp if now_ts is None else now_ts
    signal_age = now_ts - latest.timestamp
    if signal_age > config.max_signal_age_seconds:
        return _log_signal(_no_signal(sample=latest, reason="stale_signal", dry_run=dry_run))

    if last_signal_timestamp is not None and latest.timestamp - last_signal_timestamp < config.cooldown_seconds:
        return _log_signal(_no_signal(sample=latest, reason="cooldown_active", dry_run=dry_run))

    if not (config.min_entry_price <= latest.price <= config.max_entry_price):
        return _log_signal(_no_signal(sample=latest, reason="price_outside_entry_zone", dry_run=dry_run))

    window_start = latest.timestamp - config.lookback_seconds
    window_samples = [sample for sample in ordered_samples if sample.timestamp >= window_start]
    if len(window_samples) < config.min_samples:
        return _log_signal(_no_signal(sample=latest, reason="insufficient_samples", dry_run=dry_run))

    earliest = window_samples[0]
    observation_span = latest.timestamp - earliest.timestamp
    if observation_span < config.min_observation_span_seconds:
        return _log_signal(_no_signal(sample=latest, reason="observation_window_too_short", dry_run=dry_run))

    price_change = latest.price - earliest.price
    if price_change < config.min_price_change:
        return _log_signal(_no_signal(sample=latest, reason="insufficient_momentum", dry_run=dry_run))
    if price_change > config.max_price_change:
        return _log_signal(_no_signal(sample=latest, reason="chasing_extended_move", dry_run=dry_run))

    up_ticks = _count_consecutive_up_ticks(window_samples)
    if up_ticks < config.min_consecutive_up_ticks:
        return _log_signal(_no_signal(sample=latest, reason="missing_consecutive_up_ticks", dry_run=dry_run))

    velocity = price_change / max(observation_span, 1)
    if velocity < config.min_velocity_per_second:
        return _log_signal(_no_signal(sample=latest, reason="velocity_below_threshold", dry_run=dry_run))

    confidence = _compute_confidence(
        latest,
        earliest,
        price_change=price_change,
        up_ticks=up_ticks,
        velocity=velocity,
        config=config,
    )
    return _log_signal(
        MomentumSignal(
            market_id=latest.market_id,
            asset_id=latest.asset_id,
            timestamp=latest.timestamp,
            price=latest.price,
            signal_type=BUY,
            confidence=confidence,
            dry_run=dry_run,
            reason=None,
        )
    )


def generate_momentum_signal_from_db(
    *,
    asset_id: str | None = None,
    market_id: str | None = None,
    outcome: str | None = None,
    config: MomentumTriggerConfig | None = None,
    settings: Settings | None = None,
    now_ts: int | None = None,
    last_signal_timestamp: int | None = None,
    dry_run: bool = True,
    limit: int = 120,
) -> MomentumSignal:
    config = config or MomentumTriggerConfig()
    settings = settings or get_settings()
    now_ts = int(time.time()) if now_ts is None else now_ts

    with managed_connection(settings) as connection:
        samples = load_recent_trade_samples(
            connection,
            asset_id=asset_id,
            market_id=market_id,
            outcome=outcome,
            lookback_seconds=config.lookback_seconds,
            now_ts=now_ts,
            limit=limit,
        )

    return evaluate_momentum_signal(
        samples,
        config=config,
        now_ts=now_ts,
        last_signal_timestamp=last_signal_timestamp,
        dry_run=dry_run,
    )
