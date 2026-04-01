from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MonitorSnapshot:
    candidate_count: int
    profiled_count: int
    ranked_count: int
    good_trader_count: int

def _pct(num: int, denom: int) -> str:
    if denom == 0:
        return "n/a"
    return f"{num / denom:.0%}"


def _short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:6]}...{wallet[-4:]}"


def _bar(value: float, width: int = 20) -> str:
    filled = round(value * width)
    return "█" * filled + "░" * (width - filled)


def _signed_delta(current: int, previous: int) -> str:
    delta = current - previous
    return f"{delta:+d}"


def _current_snapshot(connection) -> MonitorSnapshot:
    row = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM candidate_wallets) AS candidate_count,
            (SELECT COUNT(*) FROM candidate_wallets WHERE status = 'PROFILED') AS profiled_count,
            (SELECT COUNT(*) FROM wallet_rankings) AS ranked_count,
            (
                SELECT COUNT(*)
                FROM wallet_rankings
                WHERE followability_score > 0.5
            ) AS good_trader_count
        """
    ).fetchone()
    return MonitorSnapshot(
        candidate_count=int(row["candidate_count"] or 0),
        profiled_count=int(row["profiled_count"] or 0),
        ranked_count=int(row["ranked_count"] or 0),
        good_trader_count=int(row["good_trader_count"] or 0),
    )


def _previous_snapshot(connection) -> MonitorSnapshot | None:
    row = connection.execute(
        """
        SELECT candidate_count, profiled_count, ranked_count, good_trader_count
        FROM monitor_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return MonitorSnapshot(
        candidate_count=int(row["candidate_count"] or 0),
        profiled_count=int(row["profiled_count"] or 0),
        ranked_count=int(row["ranked_count"] or 0),
        good_trader_count=int(row["good_trader_count"] or 0),
    )


def save_monitor_snapshot(settings: Settings | None = None) -> MonitorSnapshot:
    settings = settings or get_settings()
    with managed_connection(settings) as connection:
        snapshot = _current_snapshot(connection)
        connection.execute(
            """
            INSERT INTO monitor_runs(
                candidate_count, profiled_count, ranked_count, good_trader_count
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                snapshot.candidate_count,
                snapshot.profiled_count,
                snapshot.ranked_count,
                snapshot.good_trader_count,
            ),
        )
        connection.commit()
    return snapshot


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_funnel(connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM candidate_wallets
        GROUP BY status
        ORDER BY count DESC
        """
    ).fetchall()

    status_counts: dict[str, int] = {row["status"]: row["count"] for row in rows}
    total = sum(status_counts.values())

    ordered = ["NEW", "PROMOTED", "PROFILED", "RANKED", "ERROR", "PREFILTER_REJECTED"]
    lines = ["=== DISCOVERY FUNNEL ==="]
    for status in ordered:
        count = status_counts.pop(status, 0)
        if count or status in ("NEW", "PROFILED", "RANKED"):
            lines.append(f"  {status:<22} {count:>5}  {_pct(count, total):>5}")
    for status, count in status_counts.items():
        lines.append(f"  {status:<22} {count:>5}  {_pct(count, total):>5}")
    lines.append(f"  {'TOTAL':<22} {total:>5}")
    return lines


def _section_followability(connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT json_extract(metrics_json, '$.followability_score') AS score
        FROM wallet_profiles
        """
    ).fetchall()

    scores = [float(row["score"] or 0.0) for row in rows]
    if not scores:
        return ["=== FOLLOWABILITY ===", "  No profiled wallets yet."]

    buckets: dict[str, int] = {
        "> 0.7":   0,
        "0.5–0.7": 0,
        "0.3–0.5": 0,
        "0.1–0.3": 0,
        "= 0":     0,
    }
    for s in scores:
        if s > 0.7:
            buckets["> 0.7"] += 1
        elif s > 0.5:
            buckets["0.5–0.7"] += 1
        elif s > 0.3:
            buckets["0.3–0.5"] += 1
        elif s > 0.0:
            buckets["0.1–0.3"] += 1
        else:
            buckets["= 0"] += 1

    avg = sum(scores) / len(scores)
    above_threshold = sum(1 for s in scores if s > 0.5)

    lines = ["=== FOLLOWABILITY ==="]
    lines.append(f"  Wallets profiled: {len(scores)}   Avg: {avg:.3f}   Above 0.5: {above_threshold}")
    lines.append("")
    for label, count in buckets.items():
        bar = _bar(count / len(scores)) if len(scores) else ""
        lines.append(f"  {label:<10}  {count:>4}  {bar}")
    return lines


def _section_lifecycle(connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT json_extract(metrics_json, '$.lifecycle_score') AS score
        FROM wallet_profiles
        """
    ).fetchall()

    scores = [float(row["score"] or 0.0) for row in rows]
    if not scores:
        return ["=== LIFECYCLE ===", "  No profiled wallets yet."]

    buckets: dict[str, int] = {
        "> 0.7":   0,
        "0.4–0.7": 0,
        "0.2–0.4": 0,
        "< 0.2":   0,
    }
    for s in scores:
        if s > 0.7:
            buckets["> 0.7"] += 1
        elif s > 0.4:
            buckets["0.4–0.7"] += 1
        elif s > 0.2:
            buckets["0.2–0.4"] += 1
        else:
            buckets["< 0.2"] += 1

    avg = sum(scores) / len(scores)
    lines = ["=== LIFECYCLE ==="]
    lines.append(f"  Wallets profiled: {len(scores)}   Avg: {avg:.3f}")
    lines.append("")
    for label, count in buckets.items():
        bar = _bar(count / len(scores)) if len(scores) else ""
        lines.append(f"  {label:<10}  {count:>4}  {bar}")
    return lines


def _section_top_ranked(connection, limit: int = 10) -> list[str]:
    rows = connection.execute(
        """
        SELECT
            wr.wallet,
            wr.regime,
            wr.rank_score,
            wr.followability_score,
            wr.lifecycle_score,
            wr.tier,
            wp.trader_type,
            json_extract(wp.metrics_json, '$.pnl_per_minute') AS pnl_per_minute
        FROM wallet_rankings wr
        LEFT JOIN wallet_profiles wp ON wp.wallet = wr.wallet
        WHERE wr.regime = 'global'
        ORDER BY wr.rank_score DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        return ["=== TOP RANKED ===", "  No ranked wallets yet."]

    lines = ["=== TOP RANKED ==="]
    header = f"  {'wallet':<18}  {'tier':>4}  {'score':>6}  {'follow':>6}  {'lifecycle':>9}  {'pnl/min':>8}  type"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for row in rows:
        pnl_min = row["pnl_per_minute"]
        pnl_str = f"{float(pnl_min):.4f}" if pnl_min is not None else "    n/a"
        lines.append(
            f"  {_short_wallet(row['wallet']):<18}"
            f"  {row['tier']:>4}"
            f"  {row['rank_score']:>6.3f}"
            f"  {row['followability_score']:>6.3f}"
            f"  {row['lifecycle_score']:>9.3f}"
            f"  {pnl_str:>8}"
            f"  {row['trader_type'] or 'n/a'}"
        )
    return lines


def _section_emerging_traders(connection, limit: int = 10) -> list[str]:
    rows = connection.execute(
        """
        SELECT
            wallet,
            trader_type,
            json_extract(metrics_json, '$.pnl_per_minute') AS pnl_per_minute,
            json_extract(metrics_json, '$.lifecycle_score') AS lifecycle_score
        FROM wallet_profiles
        WHERE json_extract(metrics_json, '$.pnl_per_minute') > 0.02
          AND json_extract(metrics_json, '$.lifecycle_score') > 0.3
        ORDER BY json_extract(metrics_json, '$.pnl_per_minute') DESC, wallet
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        return ["=== EMERGING TRADERS ===", "  No emerging traders yet."]

    lines = ["=== EMERGING TRADERS ==="]
    header = f"  {'wallet':<18}  {'pnl/min':>8}  {'lifecycle':>9}  type"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for row in rows:
        lines.append(
            f"  {_short_wallet(row['wallet']):<18}"
            f"  {float(row['pnl_per_minute'] or 0.0):>8.4f}"
            f"  {float(row['lifecycle_score'] or 0.0):>9.3f}"
            f"  {row['trader_type'] or 'n/a'}"
        )
    return lines


def _section_pnl_distribution(connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT
            json_extract(metrics_json, '$.total_pnl') AS total_pnl,
            json_extract(metrics_json, '$.pnl_per_day') AS pnl_per_day
        FROM wallet_profiles
        """
    ).fetchall()

    if not rows:
        return ["=== PNL DISTRIBUTION ===", "  No profiled wallets yet."]

    total_pnls = [float(row["total_pnl"] or 0.0) for row in rows]
    pnl_per_days = [float(row["pnl_per_day"] or 0.0) for row in rows]

    total_buckets = {
        "> $1000": 0,
        "$100-$1000": 0,
        "< $100": 0,
    }
    for value in total_pnls:
        if value > 1000.0:
            total_buckets["> $1000"] += 1
        elif value >= 100.0:
            total_buckets["$100-$1000"] += 1
        else:
            total_buckets["< $100"] += 1

    per_day_buckets = {
        "> $200": 0,
        "$50-$200": 0,
        "< $50": 0,
    }
    for value in pnl_per_days:
        if value > 200.0:
            per_day_buckets["> $200"] += 1
        elif value >= 50.0:
            per_day_buckets["$50-$200"] += 1
        else:
            per_day_buckets["< $50"] += 1

    lines = ["=== PNL DISTRIBUTION ==="]
    lines.append(f"  Wallets profiled: {len(rows)}")
    lines.append("")
    lines.append("  Total PnL:")
    for label, count in total_buckets.items():
        lines.append(f"    {label:<10} {count:>4}")
    lines.append("")
    lines.append("  PnL/day:")
    for label, count in per_day_buckets.items():
        lines.append(f"    {label:<10} {count:>4}")
    return lines


def _section_trader_types(connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT trader_type, COUNT(*) AS count
        FROM wallet_profiles
        GROUP BY trader_type
        ORDER BY count DESC
        """
    ).fetchall()

    if not rows:
        return ["=== TRADER TYPES ===", "  No profiled wallets yet."]

    total = sum(row["count"] for row in rows)
    lines = ["=== TRADER TYPES ==="]
    for row in rows:
        pct = f"{row['count'] / total:.0%}"
        bar = _bar(row["count"] / total, width=15)
        lines.append(f"  {row['trader_type']:<25}  {row['count']:>4}  {pct:>4}  {bar}")
    return lines


def _section_microstructure(connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT
            wallet,
            COUNT(*) AS total_trades,
            SUM(CASE WHEN snapshot_id IS NOT NULL THEN 1 ELSE 0 END) AS trades_with_snapshot
        FROM trade_entry_context
        GROUP BY wallet
        """
    ).fetchall()

    if not rows:
        return ["=== MICROSTRUCTURE COVERAGE ===", "  No trade context data yet."]

    coverage_rates = [
        row["trades_with_snapshot"] / row["total_trades"]
        for row in rows
        if row["total_trades"] > 0
    ]
    avg_cov = sum(coverage_rates) / len(coverage_rates) if coverage_rates else 0.0
    low_cov = sum(1 for c in coverage_rates if c < 0.3)
    high_cov = sum(1 for c in coverage_rates if c >= 0.7)

    lines = ["=== MICROSTRUCTURE COVERAGE ==="]
    lines.append(f"  Wallets with context data: {len(coverage_rates)}")
    lines.append(f"  Avg coverage:              {avg_cov:.2%}")
    lines.append(f"  High coverage  (≥70%):     {high_cov}  wallets")
    lines.append(f"  Low coverage   (<30%):     {low_cov}  wallets  {'⚠' if low_cov > len(coverage_rates) * 0.4 else ''}")
    return lines


def _section_storage(connection, settings: Settings) -> list[str]:
    db_path = Path(settings.database_path)
    try:
        db_size_mb = db_path.stat().st_size / 1_048_576
    except OSError:
        db_size_mb = 0.0

    counts = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM microstructure_raw_book)  AS raw_count,
            (SELECT COUNT(*) FROM microstructure_agg_1m)    AS micro_agg_count,
            (SELECT COUNT(*) FROM market_price_history)     AS price_raw_count,
            (SELECT COUNT(*) FROM price_history_agg_1m)     AS price_agg_count
        """
    ).fetchone()

    lines = ["=== STORAGE ==="]
    lines.append(f"  DB size:                    {db_size_mb:.2f} MB")
    lines.append(f"  microstructure_raw_book:   {int(counts['raw_count'] or 0):,}")
    lines.append(f"  microstructure_agg_1m:     {int(counts['micro_agg_count'] or 0):,}")
    lines.append(f"  market_price_history:      {int(counts['price_raw_count'] or 0):,}")
    lines.append(f"  price_history_agg_1m:      {int(counts['price_agg_count'] or 0):,}")

    raw_count = int(counts["raw_count"] or 0)
    agg_count = int(counts["micro_agg_count"] or 0)
    if raw_count == 0 and agg_count == 0:
        lines.append("  micro retention check:     no data yet")
    elif raw_count > 0 and agg_count == 0:
        lines.append("  micro retention check:     agg empty -> check aggregation")
    elif agg_count > 0 and raw_count > agg_count * 500:
        lines.append("  micro retention check:     raw dominating -> check cleanup")
    else:
        lines.append("  micro retention check:     healthy")

    try:
        dbstat_rows = connection.execute(
            """
            SELECT name, SUM(pgsize) AS size_bytes
            FROM dbstat
            GROUP BY name
            ORDER BY size_bytes DESC, name
            LIMIT 8
            """
        ).fetchall()
    except Exception:
        dbstat_rows = []

    if dbstat_rows:
        lines.append("")
        lines.append("  Top table sizes:")
        for row in dbstat_rows:
            size_mb = float(row["size_bytes"] or 0) / 1_048_576
            lines.append(f"    {row['name']:<28} {size_mb:>8.2f} MB")
    else:
        lines.append("  Top table sizes:           unavailable")

    return lines


def _section_progress(connection) -> list[str]:
    current = _current_snapshot(connection)
    previous = _previous_snapshot(connection)

    lines = ["=== PROGRESS ==="]
    if previous is None:
        lines.append("  No previous monitor run yet.")
        lines.append(f"  Candidates:   {current.candidate_count}")
        lines.append(f"  Profiled:     {current.profiled_count}")
        lines.append(f"  Ranked:       {current.ranked_count}")
        lines.append(f"  Good traders: {current.good_trader_count}")
        return lines

    lines.append(f"  Candidates:   {current.candidate_count:>5}   ({_signed_delta(current.candidate_count, previous.candidate_count)})")
    lines.append(f"  Profiled:     {current.profiled_count:>5}   ({_signed_delta(current.profiled_count, previous.profiled_count)})")
    lines.append(f"  Ranked:       {current.ranked_count:>5}   ({_signed_delta(current.ranked_count, previous.ranked_count)})")
    lines.append(f"  Good traders: {current.good_trader_count:>5}   ({_signed_delta(current.good_trader_count, previous.good_trader_count)})")
    return lines


# ---------------------------------------------------------------------------
# Main report builder
# ---------------------------------------------------------------------------

def build_monitor_report(settings: Settings | None = None, *, top_n: int = 10) -> str:
    settings = settings or get_settings()

    with managed_connection(settings) as connection:
        sections = [
            _section_progress(connection),
            _section_funnel(connection),
            _section_followability(connection),
            _section_lifecycle(connection),
            _section_pnl_distribution(connection),
            _section_trader_types(connection),
            _section_top_ranked(connection, limit=top_n),
            _section_emerging_traders(connection, limit=top_n),
            _section_microstructure(connection),
            _section_storage(connection, settings),
        ]

    lines: list[str] = []
    for section in sections:
        lines.extend(section)
        lines.append("")
    return "\n".join(lines).rstrip()


def print_monitor(settings: Settings | None = None, *, top_n: int = 10) -> None:
    settings = settings or get_settings()
    print(build_monitor_report(settings=settings, top_n=top_n))
    try:
        save_monitor_snapshot(settings)
    except sqlite3.OperationalError as error:
        if "database is locked" not in str(error).lower():
            raise
        print("[WARNING] monitor snapshot skipped: database is locked")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor TIP V1 discovery and ranking state.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--top", type=int, default=10, help="Number of top wallets to show.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    print_monitor(settings=settings, top_n=args.top)


if __name__ == "__main__":
    main()
