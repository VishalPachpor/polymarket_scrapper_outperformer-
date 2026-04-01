from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import math

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


ENTRY_BUCKET_ORDER = ("<0.10", "0.10-0.30", "0.30-0.50", "0.50-0.70", ">0.70")
MFE_CAPTURE_BUCKET_ORDER = (">0.80", "0.50-0.80", "<0.50")
TIME_TO_MFE_BUCKET_ORDER = ("<60s", "1-3m", "3-10m", ">10m")
TIME_AFTER_MFE_BUCKET_ORDER = ("<1m", "1-5m", ">5m")


@dataclass(frozen=True)
class BucketStat:
    dimension: str
    bucket: str
    count: int
    wins: int
    losses: int
    winner_rate: float
    loser_rate: float
    win_rate: float
    avg_pnl: float
    avg_pnl_per_minute: float
    avg_mfe: float
    avg_mae: float
    avg_duration: float
    winner_avg_pnl: float
    loser_avg_pnl: float
    winner_avg_mfe: float
    loser_avg_mae: float
    early_mfe_hit_rate: float
    mfe_giveback_ratio: float
    edge_score: float
    weighted_edge_score: float
    frequency: float


@dataclass(frozen=True)
class RegimeEdgeReport:
    regime: str
    wallets_used: tuple[str, ...]
    total_positions: int
    bucket_stats: dict[str, tuple[BucketStat, ...]]
    edges: tuple[BucketStat, ...]
    failures: tuple[BucketStat, ...]


def entry_bucket(price: float | None) -> str:
    value = float(price or 0.0)
    if value < 0.10:
        return "<0.10"
    if value < 0.30:
        return "0.10-0.30"
    if value < 0.50:
        return "0.30-0.50"
    if value < 0.70:
        return "0.50-0.70"
    return ">0.70"


def mfe_capture_ratio(mfe: float | None, pnl: float | None) -> float:
    mfe_value = float(mfe or 0.0)
    pnl_value = float(pnl or 0.0)
    if mfe_value <= 0:
        return 0.0
    return pnl_value / mfe_value


def mfe_capture_bucket(mfe: float | None, pnl: float | None) -> str:
    capture = mfe_capture_ratio(mfe, pnl)
    if capture < 0.50:
        return "<0.50"
    if capture <= 0.80:
        return "0.50-0.80"
    return ">0.80"


def time_to_mfe_bucket(time_seconds: float | None) -> str:
    value = float(time_seconds or 0.0)
    if value < 60:
        return "<60s"
    if value < 180:
        return "1-3m"
    if value < 600:
        return "3-10m"
    return ">10m"


def hold_bucket(duration_seconds: float | None) -> str:
    value = float(duration_seconds or 0.0)
    if value < 300:
        return "<1m" if value < 60 else "1-5m"
    return ">5m"


def time_after_mfe_bucket(duration_seconds: float | None, time_to_mfe: float | None) -> str:
    duration = float(duration_seconds or 0.0)
    to_mfe = float(time_to_mfe or 0.0)
    return hold_bucket(max(0.0, duration - to_mfe))


def _bucket_sort_key(dimension: str, bucket: str) -> tuple[int, str]:
    orders = {
        "entry_price": ENTRY_BUCKET_ORDER,
        "mfe_capture": MFE_CAPTURE_BUCKET_ORDER,
        "time_to_mfe": TIME_TO_MFE_BUCKET_ORDER,
        "time_after_mfe": TIME_AFTER_MFE_BUCKET_ORDER,
    }
    order = orders.get(dimension, ())
    try:
        return (order.index(bucket), bucket)
    except ValueError:
        return (len(order), bucket)


def get_top_wallets(
    connection,
    regime: str,
    *,
    wallet_limit: int = 10,
    min_followability_score: float = 0.6,
    min_lifecycle_score: float = 0.4,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT wallet
        FROM wallet_rankings
        WHERE regime = ?
          AND followability_score > ?
          AND lifecycle_score > ?
        ORDER BY rank_score DESC, wallet
        LIMIT ?
        """,
        (regime, min_followability_score, min_lifecycle_score, wallet_limit),
    ).fetchall()
    return [str(row["wallet"]) for row in rows]


def get_emerging_wallets(
    connection,
    regime: str,
    *,
    wallet_limit: int = 10,
    min_pnl_per_minute: float = 0.02,
    min_lifecycle_score: float = 0.3,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT wp.wallet
        FROM wallet_profiles wp
        JOIN candidate_wallet_sources cws
          ON cws.wallet = wp.wallet
        WHERE cws.regime = ?
          AND json_extract(wp.metrics_json, '$.pnl_per_minute') > ?
          AND json_extract(wp.metrics_json, '$.lifecycle_score') > ?
        GROUP BY wp.wallet
        ORDER BY
            json_extract(wp.metrics_json, '$.pnl_per_minute') DESC,
            wp.wallet
        LIMIT ?
        """,
        (regime, min_pnl_per_minute, min_lifecycle_score, wallet_limit),
    ).fetchall()
    return [str(row["wallet"]) for row in rows]


def load_closed_positions(connection, wallets: list[str], *, version: int = 1) -> list[dict[str, float | int | str | None]]:
    if not wallets:
        return []

    placeholders = ",".join("?" for _ in wallets)
    rows = connection.execute(
        f"""
        SELECT
            p.id AS position_id,
            p.wallet,
            p.market_id,
            p.outcome,
            p.entry_price,
            p.exit_price,
            p.size,
            p.pnl,
            p.duration,
            pm.mfe,
            pm.mae,
            pm.time_to_mfe,
            tec.spread,
            tec.imbalance,
            tec.price_change_30s,
            tec.volatility_30s
        FROM positions_reconstructed p
        LEFT JOIN position_path_metrics pm
            ON pm.position_id = p.id
        LEFT JOIN trade_entry_context tec
            ON tec.trade_event_id = p.entry_trade_event_id
        WHERE p.wallet IN ({placeholders})
          AND p.status = 'CLOSED'
          AND p.version = ?
        ORDER BY p.wallet, p.entry_time, p.id
        """,
        [*wallets, version],
    ).fetchall()
    return [dict(row) for row in rows]


def _aggregate_dimension(rows: list[dict[str, float | int | str | None]], dimension: str) -> tuple[BucketStat, ...]:
    buckets: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "count": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": 0.0,
            "total_mfe": 0.0,
            "total_mae": 0.0,
            "total_duration": 0.0,
            "winner_total_pnl": 0.0,
            "loser_total_pnl": 0.0,
            "winner_total_mfe": 0.0,
            "loser_total_mae": 0.0,
            "early_mfe_hits": 0.0,
            "total_giveback_ratio": 0.0,
            "positive_mfe_count": 0,
        }
    )

    for row in rows:
        if dimension == "entry_price":
            bucket = entry_bucket(row["entry_price"])
        elif dimension == "mfe_capture":
            bucket = mfe_capture_bucket(row["mfe"], row["pnl"])
        elif dimension == "time_to_mfe":
            bucket = time_to_mfe_bucket(row["time_to_mfe"])
        elif dimension == "time_after_mfe":
            bucket = time_after_mfe_bucket(row["duration"], row["time_to_mfe"])
        else:
            raise ValueError(f"Unsupported dimension: {dimension}")

        pnl = float(row["pnl"] or 0.0)
        mfe = float(row["mfe"] or 0.0)
        mae = float(row["mae"] or 0.0)
        duration = float(row["duration"] or 0.0)
        pnl_per_minute = pnl / (duration / 60.0) if duration > 0 else 0.0
        time_to_mfe = float(row["time_to_mfe"] or 0.0)

        aggregate = buckets[bucket]
        aggregate["count"] += 1
        aggregate["total_pnl"] += pnl
        aggregate.setdefault("total_pnl_per_minute", 0.0)
        aggregate["total_pnl_per_minute"] += pnl_per_minute
        aggregate["total_mfe"] += mfe
        aggregate["total_mae"] += mae
        aggregate["total_duration"] += duration
        if mfe > 0 and time_to_mfe <= 90:
            aggregate["early_mfe_hits"] += 1
        if mfe > 0:
            aggregate["positive_mfe_count"] += 1
            aggregate["total_giveback_ratio"] += max(0.0, (mfe - pnl) / mfe)
        if pnl > 0:
            aggregate["wins"] += 1
            aggregate["winner_total_pnl"] += pnl
            aggregate["winner_total_mfe"] += mfe
        elif pnl < 0:
            aggregate["losses"] += 1
            aggregate["loser_total_pnl"] += pnl
            aggregate["loser_total_mae"] += abs(mae)

    total = max(1, len(rows))
    stats = []
    for bucket, values in buckets.items():
        count = int(values["count"])
        wins = int(values["wins"])
        losses = int(values["losses"])
        winner_rate = (wins / count) if count else 0.0
        loser_rate = (losses / count) if count else 0.0
        winner_avg_pnl = (float(values["winner_total_pnl"]) / wins) if wins else 0.0
        loser_avg_pnl = (float(values["loser_total_pnl"]) / losses) if losses else 0.0
        winner_avg_mfe = (float(values["winner_total_mfe"]) / wins) if wins else 0.0
        loser_avg_mae = (float(values["loser_total_mae"]) / losses) if losses else 0.0
        edge_score = (winner_rate * winner_avg_mfe) - (loser_rate * loser_avg_mae)
        weighted_edge_score = edge_score * math.log1p(count)
        positive_mfe_count = int(values["positive_mfe_count"])
        stats.append(
            BucketStat(
                dimension=dimension,
                bucket=bucket,
                count=count,
                wins=wins,
                losses=losses,
                winner_rate=winner_rate,
                loser_rate=loser_rate,
                win_rate=winner_rate,
                avg_pnl=float(values["total_pnl"]) / count if count else 0.0,
                avg_pnl_per_minute=float(values["total_pnl_per_minute"]) / count if count else 0.0,
                avg_mfe=float(values["total_mfe"]) / count if count else 0.0,
                avg_mae=float(values["total_mae"]) / count if count else 0.0,
                avg_duration=float(values["total_duration"]) / count if count else 0.0,
                winner_avg_pnl=winner_avg_pnl,
                loser_avg_pnl=loser_avg_pnl,
                winner_avg_mfe=winner_avg_mfe,
                loser_avg_mae=loser_avg_mae,
                early_mfe_hit_rate=(float(values["early_mfe_hits"]) / count) if count else 0.0,
                mfe_giveback_ratio=(
                    float(values["total_giveback_ratio"]) / positive_mfe_count
                    if positive_mfe_count
                    else 0.0
                ),
                edge_score=edge_score,
                weighted_edge_score=weighted_edge_score,
                frequency=float(count) / total,
            )
        )

    return tuple(sorted(stats, key=lambda stat: _bucket_sort_key(stat.dimension, stat.bucket)))


def _collect_bucket_stats(rows: list[dict[str, float | int | str | None]]) -> dict[str, tuple[BucketStat, ...]]:
    dimensions = (
        "entry_price",
        "mfe_capture",
        "time_to_mfe",
        "time_after_mfe",
    )
    return {dimension: _aggregate_dimension(rows, dimension) for dimension in dimensions}


def _extract_patterns(
    bucket_stats: dict[str, tuple[BucketStat, ...]],
    *,
    min_bucket_count: int = 3,
    edge_win_rate: float = 0.65,
    edge_avg_pnl: float = 0.5,
    failure_win_rate: float = 0.45,
    failure_avg_pnl: float = -0.5,
) -> tuple[tuple[BucketStat, ...], tuple[BucketStat, ...]]:
    all_stats = [stat for stats in bucket_stats.values() for stat in stats]

    edges = tuple(
        sorted(
            (
                stat for stat in all_stats
                if stat.count >= min_bucket_count
                and stat.win_rate >= edge_win_rate
                and stat.avg_pnl > edge_avg_pnl
            ),
            key=lambda stat: (-stat.weighted_edge_score, -stat.edge_score, -stat.win_rate, -stat.count, stat.dimension, stat.bucket),
        )
    )
    failures = tuple(
        sorted(
            (
                stat for stat in all_stats
                if stat.count >= min_bucket_count
                and stat.win_rate <= failure_win_rate
                and stat.avg_pnl < failure_avg_pnl
            ),
            key=lambda stat: (stat.weighted_edge_score, stat.edge_score, stat.avg_pnl, stat.win_rate, -stat.count, stat.dimension, stat.bucket),
        )
    )
    return edges, failures


def extract_regime_edges(
    regime: str,
    settings: Settings | None = None,
    *,
    selection_mode: str = "ranked",
    wallet_limit: int = 10,
    min_followability_score: float = 0.6,
    min_lifecycle_score: float = 0.4,
    min_pnl_per_minute: float = 0.02,
    version: int = 1,
    min_bucket_count: int = 3,
) -> RegimeEdgeReport:
    settings = settings or get_settings()

    with managed_connection(settings) as connection:
        if selection_mode == "emerging":
            wallets = get_emerging_wallets(
                connection,
                regime,
                wallet_limit=wallet_limit,
                min_pnl_per_minute=min_pnl_per_minute,
                min_lifecycle_score=min_lifecycle_score,
            )
        else:
            wallets = get_top_wallets(
                connection,
                regime,
                wallet_limit=wallet_limit,
                min_followability_score=min_followability_score,
                min_lifecycle_score=min_lifecycle_score,
            )
        rows = load_closed_positions(connection, wallets, version=version)

    bucket_stats = _collect_bucket_stats(rows)
    edges, failures = _extract_patterns(
        bucket_stats,
        min_bucket_count=min_bucket_count,
    )
    return RegimeEdgeReport(
        regime=regime,
        wallets_used=tuple(wallets),
        total_positions=len(rows),
        bucket_stats=bucket_stats,
        edges=edges,
        failures=failures,
    )


def render_regime_edge_report(report: RegimeEdgeReport) -> str:
    def add_dimension_section(lines: list[str], dimension: str) -> None:
        stats = report.bucket_stats.get(dimension, ())
        if not stats:
            lines.append("No bucket data.")
            return
        for stat in stats:
            lines.append(
                f"- {stat.bucket:<10} | count={stat.count:<4} | "
                f"win={stat.winner_rate:.2%} | lose={stat.loser_rate:.2%} | "
                f"w_mfe={stat.winner_avg_mfe:+.2f} | l_mae={stat.loser_avg_mae:+.2f} | "
                f"fast_mfe={stat.early_mfe_hit_rate:.2%} | giveback={stat.mfe_giveback_ratio:.2%} | "
                f"score={stat.weighted_edge_score:+.3f}"
            )

    def best_bucket(dimension: str) -> BucketStat | None:
        stats = report.bucket_stats.get(dimension, ())
        eligible = [stat for stat in stats if stat.count > 0]
        if not eligible:
            return None
        return max(eligible, key=lambda stat: (stat.weighted_edge_score, stat.edge_score, stat.avg_pnl))

    def worst_bucket(dimension: str) -> BucketStat | None:
        stats = report.bucket_stats.get(dimension, ())
        eligible = [stat for stat in stats if stat.count > 0]
        if not eligible:
            return None
        return min(eligible, key=lambda stat: (stat.weighted_edge_score, stat.edge_score, stat.avg_pnl))

    lines = [
        f"REGIME: {report.regime}",
        f"WALLETS ANALYZED: {len(report.wallets_used)}",
        f"CLOSED POSITIONS: {report.total_positions}",
        "",
        "=== ENTRY EDGE ===",
    ]
    add_dimension_section(lines, "entry_price")
    lines.extend(["", "=== TIMING EDGE ==="])
    add_dimension_section(lines, "time_to_mfe")
    lines.extend(["", "=== PATH EDGE ==="])
    add_dimension_section(lines, "mfe_capture")
    lines.extend(["", "=== EXIT FAILURE ==="])
    add_dimension_section(lines, "time_after_mfe")

    actionable: list[str] = []
    entry_best = best_bucket("entry_price")
    if entry_best:
        actionable.append(f"enter bias: prefer {entry_best.bucket} entries (score {entry_best.weighted_edge_score:+.3f})")
    timing_best = best_bucket("time_to_mfe")
    if timing_best:
        actionable.append(f"edge realization: strongest trades peak in {timing_best.bucket}")
    exit_worst = worst_bucket("time_after_mfe")
    if exit_worst:
        actionable.append(f"exit guardrail: avoid holding {exit_worst.bucket} after MFE")
    path_worst = worst_bucket("mfe_capture")
    if path_worst:
        actionable.append(f"capture leak: buckets like {path_worst.bucket} give back {path_worst.mfe_giveback_ratio:.0%} of MFE")

    lines.extend(["", "=== ACTIONABLE ==="])
    if actionable:
        for item in actionable:
            lines.append(f"- {item}")
    else:
        lines.append("No actionable pattern summary yet.")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract repeatable edge patterns by regime.")
    parser.add_argument("--regime", required=True, help="Regime key, e.g. crypto_5m")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument(
        "--selection-mode",
        choices=("ranked", "emerging"),
        default="ranked",
        help="Use followable ranked wallets or high-pnl emerging wallets.",
    )
    parser.add_argument("--wallet-limit", type=int, default=10)
    parser.add_argument("--min-followability-score", type=float, default=0.6)
    parser.add_argument("--min-lifecycle-score", type=float, default=0.4)
    parser.add_argument("--min-pnl-per-minute", type=float, default=0.02)
    parser.add_argument("--version", type=int, default=1)
    parser.add_argument("--min-bucket-count", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    report = extract_regime_edges(
        args.regime,
        settings=settings,
        selection_mode=args.selection_mode,
        wallet_limit=args.wallet_limit,
        min_followability_score=args.min_followability_score,
        min_lifecycle_score=args.min_lifecycle_score,
        min_pnl_per_minute=args.min_pnl_per_minute,
        version=args.version,
        min_bucket_count=args.min_bucket_count,
    )
    print(render_regime_edge_report(report))


if __name__ == "__main__":
    main()
