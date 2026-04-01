from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.discovery.leaderboard import fetch_default_leaderboard_wallets
from tip_v1.discovery.market_scanner import scan_market_regime
from tip_v1.discovery.prefilter import (
    compute_prefilter_score,
    compute_sample_stats,
    passes_prefilter,
)
from tip_v1.discovery.trade_scanner import (
    extract_wallets_from_recent_trades,
    fetch_recent_trades,
    fetch_wallet_trades,
)
from tip_v1.main import run_pipeline
from tip_v1.profiler.wallet_profiler import load_stored_profile, profile_wallet


@dataclass(frozen=True)
class DiscoveryCycleResult:
    discovered_count: int
    processed_count: int
    promoted_count: int
    profiled_count: int
    ranked_count: int


SOURCE_WEIGHTS = {
    "leaderboard": 1.0,
    "trade_stream": 0.6,
    "market_scanner": 0.9,
}


def _utc_iso(dt: datetime | None = None) -> str:
    value = dt or datetime.now(timezone.utc)
    return value.isoformat()


def upsert_candidate(connection, wallet: str) -> None:
    connection.execute(
        """
        INSERT INTO candidate_wallets(wallet)
        VALUES (?)
        ON CONFLICT(wallet) DO NOTHING
        """,
        (wallet.lower(),),
    )


def insert_source(
    connection,
    wallet: str,
    source: str,
    *,
    regime: str = "global",
    source_rank: int | None = None,
    source_metadata: dict | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_wallet_sources(wallet, source, regime, source_rank, source_metadata_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            wallet.lower(),
            source,
            regime,
            source_rank,
            json.dumps(source_metadata or {}, sort_keys=True, separators=(",", ":")),
        ),
    )


def discover_wallets(
    settings: Settings,
    *,
    leaderboard_limit: int = 50,
    recent_trade_limit: int = 200,
    recent_trade_min_count: int = 5,
    market_scan_limit: int = 20,
    market_trade_limit: int = 100,
) -> int:
    leaderboard_wallets = []
    recent_trade_wallets = []
    market_wallets = []

    try:
        leaderboard_wallets = fetch_default_leaderboard_wallets(limit=leaderboard_limit)
    except Exception as error:
        print(
            f"[WARNING] Leaderboard discovery skipped: {error}",
            file=sys.stderr,
        )

    try:
        recent_trade_wallets = extract_wallets_from_recent_trades(
            fetch_recent_trades(settings=settings, limit=recent_trade_limit),
            min_trade_count=recent_trade_min_count,
        )
    except Exception as error:
        print(
            f"[WARNING] Trade-stream discovery skipped: {error}",
            file=sys.stderr,
        )

    try:
        market_wallets = scan_market_regime(
            settings=settings,
            category="crypto",
            time_bucket="5m",
            market_limit=market_scan_limit,
            trade_limit=market_trade_limit,
        )
    except Exception as error:
        print(
            f"[WARNING] Market-scanner discovery skipped: {error}",
            file=sys.stderr,
        )

    with managed_connection(settings) as connection:
        before = connection.execute(
            "SELECT COUNT(*) AS count FROM candidate_wallets"
        ).fetchone()["count"]

        for row in leaderboard_wallets:
            upsert_candidate(connection, row.wallet)
            insert_source(
                connection,
                row.wallet,
                row.source,
                regime=row.regime,
                source_rank=row.source_rank,
                source_metadata=row.source_metadata,
            )

        for row in recent_trade_wallets:
            upsert_candidate(connection, row.wallet)
            insert_source(
                connection,
                row.wallet,
                row.source,
                regime=row.regime,
                source_rank=row.source_rank,
                source_metadata=row.source_metadata,
            )

        for row in market_wallets:
            upsert_candidate(connection, row.wallet)
            insert_source(
                connection,
                row.wallet,
                row.source,
                regime=row.regime,
                source_rank=row.source_rank,
                source_metadata=row.source_metadata,
            )

        after = connection.execute(
            "SELECT COUNT(*) AS count FROM candidate_wallets"
        ).fetchone()["count"]
        connection.commit()

    return int(after) - int(before)


def _due_candidate_wallets(settings: Settings, limit: int) -> list[str]:
    with managed_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT cw.wallet
            FROM candidate_wallets cw
            LEFT JOIN (
                SELECT
                    wallet,
                    COUNT(DISTINCT source) AS source_count,
                    SUM(
                        CASE
                            WHEN source LIKE 'leaderboard%' THEN 1.0
                            WHEN source = 'market_scanner' THEN 0.9
                            WHEN source = 'trade_stream' THEN 0.6
                            ELSE 0.0
                        END
                    ) AS source_weight_score
                FROM candidate_wallet_sources
                GROUP BY wallet
            ) src ON src.wallet = cw.wallet
            WHERE cw.next_check_at IS NULL OR cw.next_check_at <= CURRENT_TIMESTAMP
            ORDER BY COALESCE(src.source_weight_score, 0) DESC,
                     COALESCE(src.source_count, 0) DESC,
                     cw.discovered_at,
                     cw.wallet
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [str(row["wallet"]) for row in rows]


def _source_count(settings: Settings, wallet: str) -> int:
    with managed_connection(settings) as connection:
        row = connection.execute(
            """
            SELECT COUNT(DISTINCT source) AS source_count
            FROM candidate_wallet_sources
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchone()
    return int(row["source_count"] or 0)


def _wallet_regimes(settings: Settings, wallet: str) -> list[str]:
    with managed_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT regime
            FROM candidate_wallet_sources
            WHERE wallet = ?
            ORDER BY regime
            """,
            (wallet,),
        ).fetchall()
    regimes = [str(row["regime"]) for row in rows if row["regime"]]
    return regimes or ["global"]


def _update_prefilter_result(
    settings: Settings,
    wallet: str,
    *,
    stats: dict[str, int],
    score: float,
    passed: bool,
    reason: str,
) -> None:
    next_check = (
        datetime.now(timezone.utc) + timedelta(hours=2)
        if passed
        else datetime.now(timezone.utc) + timedelta(days=1)
    )
    with managed_connection(settings) as connection:
        connection.execute(
            """
            UPDATE candidate_wallets
            SET
                last_checked_at = CURRENT_TIMESTAMP,
                next_check_at = ?,
                sample_trade_count = ?,
                sample_buy_count = ?,
                sample_sell_count = ?,
                sample_unique_markets = ?,
                prefilter_score = ?,
                prefilter_passed = ?,
                prefilter_reason = ?,
                status = ?
            WHERE wallet = ?
            """,
            (
                _utc_iso(next_check),
                stats["trade_count"],
                stats["buy_count"],
                stats["sell_count"],
                stats["unique_markets"],
                score,
                int(passed),
                reason,
                "PROMOTED" if passed else "PREFILTER_REJECTED",
                wallet,
            ),
        )
        connection.commit()


def _mark_profiled(settings: Settings, wallet: str) -> None:
    with managed_connection(settings) as connection:
        connection.execute(
            """
            UPDATE candidate_wallets
            SET status = 'PROFILED', profiled_at = CURRENT_TIMESTAMP
            WHERE wallet = ?
            """,
            (wallet,),
        )
        connection.commit()


def _mark_error(settings: Settings, wallet: str, error: Exception) -> None:
    with managed_connection(settings) as connection:
        connection.execute(
            """
            UPDATE candidate_wallets
            SET
                status = 'ERROR',
                last_error = ?,
                check_error_count = check_error_count + 1,
                next_check_at = ?
            WHERE wallet = ?
            """,
            (
                str(error),
                _utc_iso(datetime.now(timezone.utc) + timedelta(hours=6)),
                wallet,
            ),
        )
        connection.commit()


def promote_wallet(settings: Settings, wallet: str) -> None:
    # Discovery needs fast lifecycle completion signals first; targeted path enrichment
    # can be run later on shortlisted wallets without stalling the whole queue.
    run_pipeline(wallet, settings=settings, skip_enrichment=True)
    profile_wallet(wallet, settings=settings)
    _mark_profiled(settings, wallet)


def process_candidates(
    settings: Settings,
    *,
    limit: int = 20,
    prefilter_trade_limit: int = 100,
) -> tuple[int, int]:
    processed = 0
    promoted = 0
    for wallet in _due_candidate_wallets(settings, limit):
        try:
            trades = fetch_wallet_trades(
                wallet,
                settings=settings,
                limit=prefilter_trade_limit,
            )
            stats = compute_sample_stats(trades)
            score = compute_prefilter_score(
                stats,
                multi_source=_source_count(settings, wallet) > 1,
            )
            passed, reason = passes_prefilter(stats)
            _update_prefilter_result(
                settings,
                wallet,
                stats=stats,
                score=score,
                passed=passed,
                reason=reason,
            )
            processed += 1

            if passed:
                promote_wallet(settings, wallet)
                promoted += 1
        except Exception as error:
            _mark_error(settings, wallet, error)

    return processed, promoted


def update_rankings(settings: Settings) -> int:
    with managed_connection(settings) as connection:
        wallets = [
            str(row["wallet"])
            for row in connection.execute(
                """
                SELECT wallet
                FROM candidate_wallets
                WHERE status = 'PROFILED'
                ORDER BY profiled_at, wallet
                """
            ).fetchall()
        ]

    ranked = 0
    for wallet in wallets:
        profile = load_stored_profile(wallet, settings=settings)
        if not profile:
            continue
        followability_score = float(profile["followability_score"])
        confidence = float(profile["confidence"])
        lifecycle_score = float(profile["metrics"].get("lifecycle_score", 0.0))
        if lifecycle_score <= 0.2:
            with managed_connection(settings) as connection:
                connection.execute(
                    "DELETE FROM wallet_rankings WHERE wallet = ?",
                    (wallet,),
                )
                connection.commit()
            continue

        profit_factor = float(profile["metrics"].get("profit_factor", 0.0))
        pnl_per_minute_score = float(profile["metrics"].get("pnl_per_minute_score", 0.0))
        total_trades = int(profile["features"].get("total_trades", 0))
        activity = min(1.0, math.log1p(max(0, total_trades)) / math.log(201))
        consistency = confidence
        profit_factor_score = min(max(profit_factor, 0.0), 3.0) / 3.0
        rank_score = round(
            0.30 * followability_score
            + 0.20 * lifecycle_score
            + 0.20 * profit_factor_score
            + 0.15 * pnl_per_minute_score
            + 0.10 * consistency
            + 0.05 * activity,
            4,
        )
        tier = (
            "A" if rank_score >= 0.7 else
            "B" if rank_score >= 0.5 else
            "C"
        )

        for regime in _wallet_regimes(settings, wallet):
            with managed_connection(settings) as connection:
                connection.execute(
                    """
                    INSERT INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(wallet, regime) DO UPDATE SET
                        followability_score = excluded.followability_score,
                        lifecycle_score = excluded.lifecycle_score,
                        rank_score = excluded.rank_score,
                        tier = excluded.tier,
                        ranked_at = CURRENT_TIMESTAMP
                    """,
                    (wallet, regime, followability_score, lifecycle_score, rank_score, tier),
                )
                connection.commit()
            ranked += 1

    return ranked


def run_discovery_cycle(
    settings: Settings | None = None,
    *,
    leaderboard_limit: int = 50,
    recent_trade_limit: int = 200,
    recent_trade_min_count: int = 5,
    market_scan_limit: int = 20,
    market_trade_limit: int = 100,
    candidate_process_limit: int = 20,
    prefilter_trade_limit: int = 100,
) -> DiscoveryCycleResult:
    settings = settings or get_settings()
    initialize_database(settings)

    discovered_count = discover_wallets(
        settings,
        leaderboard_limit=leaderboard_limit,
        recent_trade_limit=recent_trade_limit,
        recent_trade_min_count=recent_trade_min_count,
        market_scan_limit=market_scan_limit,
        market_trade_limit=market_trade_limit,
    )
    processed_count, promoted_count = process_candidates(
        settings,
        limit=candidate_process_limit,
        prefilter_trade_limit=prefilter_trade_limit,
    )
    ranked_count = update_rankings(settings)

    return DiscoveryCycleResult(
        discovered_count=discovered_count,
        processed_count=processed_count,
        promoted_count=promoted_count,
        profiled_count=promoted_count,
        ranked_count=ranked_count,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TIP V1 discovery cycle.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--leaderboard-limit", type=int, default=50)
    parser.add_argument("--recent-trade-limit", type=int, default=200)
    parser.add_argument("--recent-trade-min-count", type=int, default=5)
    parser.add_argument("--market-scan-limit", type=int, default=20)
    parser.add_argument("--market-trade-limit", type=int, default=100)
    parser.add_argument("--candidate-process-limit", type=int, default=20)
    parser.add_argument("--prefilter-trade-limit", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    result = run_discovery_cycle(
        settings,
        leaderboard_limit=args.leaderboard_limit,
        recent_trade_limit=args.recent_trade_limit,
        recent_trade_min_count=args.recent_trade_min_count,
        market_scan_limit=args.market_scan_limit,
        market_trade_limit=args.market_trade_limit,
        candidate_process_limit=args.candidate_process_limit,
        prefilter_trade_limit=args.prefilter_trade_limit,
    )
    print(
        "[DISCOVERY]"
        f" discovered={result.discovered_count}"
        f" processed={result.processed_count}"
        f" promoted={result.promoted_count}"
        f" profiled={result.profiled_count}"
        f" ranked={result.ranked_count}"
    )


if __name__ == "__main__":
    main()
