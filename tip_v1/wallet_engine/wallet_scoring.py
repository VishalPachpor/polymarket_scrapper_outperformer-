from __future__ import annotations

import json
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection
from tip_v1.wallet_engine.wallet_state import WalletActivityEvent


@dataclass(frozen=True)
class WalletBaseScore:
    wallet: str
    rank_score: float
    followability_score: float
    lifecycle_score: float
    trader_type: str
    profile_confidence: float
    pnl_per_minute: float
    pnl_per_day: float
    best_markets: tuple[str, ...]


@dataclass(frozen=True)
class WalletLiveScore:
    wallet: str
    base_score: float
    live_score: float
    confidence: str
    burst_boost: float
    recency_boost: float
    market_specialist: bool
    trader_type: str
    best_markets: tuple[str, ...]


def load_wallet_base_scores(
    *,
    settings: Settings | None = None,
    regime: str = "global",
    limit: int = 100,
    best_market_limit: int = 5,
) -> dict[str, WalletBaseScore]:
    settings = settings or get_settings()
    with managed_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT
                ranking.wallet,
                ranking.rank_score,
                ranking.followability_score,
                ranking.lifecycle_score,
                profile.trader_type,
                profile.confidence,
                profile.features_json
            FROM wallet_rankings AS ranking
            LEFT JOIN wallet_profiles AS profile
              ON profile.wallet = ranking.wallet
            WHERE ranking.regime = ?
            ORDER BY ranking.rank_score DESC
            LIMIT ?
            """,
            (regime, limit),
        ).fetchall()

        scores: dict[str, WalletBaseScore] = {}
        for row in rows:
            features = json.loads(row["features_json"]) if row["features_json"] else {}
            wallet = str(row["wallet"]).lower()
            best_markets = tuple(
                market_row["market_id"]
                for market_row in connection.execute(
                    """
                    SELECT market_id
                    FROM positions_reconstructed
                    WHERE wallet = ? AND status = 'CLOSED' AND pnl IS NOT NULL
                    GROUP BY market_id
                    ORDER BY SUM(pnl) DESC, COUNT(*) DESC, market_id
                    LIMIT ?
                    """,
                    (wallet, best_market_limit),
                ).fetchall()
            )
            scores[wallet] = WalletBaseScore(
                wallet=wallet,
                rank_score=float(row["rank_score"] or 0.0),
                followability_score=float(row["followability_score"] or 0.0),
                lifecycle_score=float(row["lifecycle_score"] or 0.0),
                trader_type=str(row["trader_type"] or "UNKNOWN"),
                profile_confidence=float(row["confidence"] or 0.0),
                pnl_per_minute=float(features.get("realized_pnl_per_minute") or 0.0),
                pnl_per_day=float(features.get("pnl_per_day") or 0.0),
                best_markets=best_markets,
            )
    return scores


def score_wallet_activity(
    base: WalletBaseScore,
    activity: WalletActivityEvent,
) -> WalletLiveScore:
    burst_boost = min(0.35, max(0.0, 0.10 * activity.burst_score))
    recency_age = max(0, activity.timestamp - activity.last_trade_ts)
    recency_boost = 0.20 if recency_age <= 5 else 0.05 if recency_age <= 15 else 0.0
    market_specialist = not base.best_markets or activity.market_id in base.best_markets
    specialist_multiplier = 1.10 if market_specialist else 0.85
    live_score = base.rank_score * (1.0 + burst_boost + recency_boost) * specialist_multiplier
    if live_score >= 0.75:
        confidence = "HIGH"
    elif live_score >= 0.50:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"
    return WalletLiveScore(
        wallet=base.wallet,
        base_score=base.rank_score,
        live_score=live_score,
        confidence=confidence,
        burst_boost=burst_boost,
        recency_boost=recency_boost,
        market_specialist=market_specialist,
        trader_type=base.trader_type,
        best_markets=base.best_markets,
    )
