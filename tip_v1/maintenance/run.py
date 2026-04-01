from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.maintenance.aggregate import AggregationResult, run_aggregation
from tip_v1.maintenance.cleanup import CleanupResult, run_cleanup


@dataclass(frozen=True)
class MaintenanceResult:
    aggregation: AggregationResult
    cleanup: CleanupResult


def _db_size_mb(settings: Settings) -> float:
    try:
        return settings.database_path.stat().st_size / 1_048_576
    except OSError:
        return 0.0


def _table_row_counts(settings: Settings) -> dict[str, int]:
    hot_tables = [
        "trades_raw",
        "trade_events",
        "positions_reconstructed",
        "microstructure_raw_book",
        "microstructure_book_snapshots",
        "market_price_history",
        "microstructure_agg_1m",
        "price_history_agg_1m",
    ]
    counts: dict[str, int] = {}
    with managed_connection(settings) as connection:
        existing = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        for table in hot_tables:
            if table in existing:
                row = connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
                counts[table] = int(row["n"])
    return counts


def run_maintenance(
    settings: Settings | None = None,
    *,
    aggregate_cutoff_hours: int = 2,
    micro_raw_max_age_hours: int = 48,
    price_history_max_age_days: int = 7,
    vacuum: bool = False,
) -> MaintenanceResult:
    """Run aggregation then cleanup in the correct order.

    Always aggregate before cleanup so no data is deleted before it is
    summarised into the agg tables.
    """
    settings = settings or get_settings()
    agg = run_aggregation(
        settings,
        older_than_seconds=aggregate_cutoff_hours * 3600,
    )
    cleanup = run_cleanup(
        settings,
        micro_raw_max_age_seconds=micro_raw_max_age_hours * 3600,
        price_history_max_age_seconds=price_history_max_age_days * 86_400,
        vacuum=vacuum,
    )
    return MaintenanceResult(aggregation=agg, cleanup=cleanup)


def print_maintenance_report(settings: Settings) -> None:
    size_before = _db_size_mb(settings)
    counts_before = _table_row_counts(settings)

    result = run_maintenance(settings, vacuum=True)

    size_after = _db_size_mb(settings)
    counts_after = _table_row_counts(settings)

    agg = result.aggregation
    cleanup = result.cleanup

    print("=== MAINTENANCE RUN ===")
    print()
    print("--- Aggregation ---")
    print(f"  microstructure_agg_1m:  +{agg.micro_minutes_inserted} new minutes  ({agg.micro_minutes_skipped} already existed)")
    print(f"  price_history_agg_1m:   +{agg.price_minutes_inserted} new minutes  ({agg.price_minutes_skipped} already existed)")
    print()
    print("--- Cleanup ---")
    print(f"  microstructure_raw_book deleted: {cleanup.raw_book_deleted:,} rows")
    print(f"  market_price_history deleted:    {cleanup.price_history_deleted:,} rows")
    print(f"  VACUUM:                          {'yes' if cleanup.vacuum_run else 'no'}")
    print()
    print("--- Storage ---")
    print(f"  DB size before: {size_before:.1f} MB")
    print(f"  DB size after:  {size_after:.1f} MB")
    print(f"  Reclaimed:      {max(0.0, size_before - size_after):.1f} MB")
    print()
    print("--- Row Counts (after) ---")
    for table, count in counts_after.items():
        before = counts_before.get(table, count)
        delta = count - before
        delta_str = f"  ({delta:+,})" if delta != 0 else ""
        print(f"  {table:<35} {count:>12,}{delta_str}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TIP V1 maintenance (aggregate + cleanup).")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--aggregate-cutoff-hours", type=int, default=2,
                        help="Aggregate data older than N hours (default: 2).")
    parser.add_argument("--micro-raw-max-age-hours", type=int, default=48,
                        help="Delete raw microstructure older than N hours (default: 48).")
    parser.add_argument("--price-history-max-age-days", type=int, default=7,
                        help="Delete raw price history older than N days (default: 7).")
    parser.add_argument("--vacuum", action="store_true",
                        help="Run VACUUM after cleanup (slow but reclaims disk space).")
    parser.add_argument("--report", action="store_true", default=True,
                        help="Print summary report (default: on).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)

    if args.report:
        size_before = _db_size_mb(settings)
        counts_before = _table_row_counts(settings)
        result = run_maintenance(
            settings,
            aggregate_cutoff_hours=args.aggregate_cutoff_hours,
            micro_raw_max_age_hours=args.micro_raw_max_age_hours,
            price_history_max_age_days=args.price_history_max_age_days,
            vacuum=args.vacuum,
        )
        size_after = _db_size_mb(settings)
        counts_after = _table_row_counts(settings)

        agg = result.aggregation
        cleanup = result.cleanup

        print("=== MAINTENANCE RUN ===")
        print()
        print("--- Aggregation ---")
        print(f"  microstructure_agg_1m  +{agg.micro_minutes_inserted} inserted  {agg.micro_minutes_skipped} skipped")
        print(f"  price_history_agg_1m   +{agg.price_minutes_inserted} inserted  {agg.price_minutes_skipped} skipped")
        print()
        print("--- Cleanup ---")
        print(f"  microstructure_raw_book  {cleanup.raw_book_deleted:>8,} rows deleted")
        print(f"  market_price_history     {cleanup.price_history_deleted:>8,} rows deleted")
        print(f"  VACUUM                   {'yes' if cleanup.vacuum_run else 'no'}")
        print()
        print("--- Storage ---")
        print(f"  Before: {size_before:.1f} MB   After: {size_after:.1f} MB   Reclaimed: {max(0.0, size_before - size_after):.1f} MB")
        print()
        print("--- Row Counts ---")
        for table, count in counts_after.items():
            before = counts_before.get(table, count)
            delta = count - before
            delta_str = f" ({delta:+,})" if delta != 0 else ""
            print(f"  {table:<35} {count:>10,}{delta_str}")
    else:
        run_maintenance(
            settings,
            aggregate_cutoff_hours=args.aggregate_cutoff_hours,
            micro_raw_max_age_hours=args.micro_raw_max_age_hours,
            price_history_max_age_days=args.price_history_max_age_days,
            vacuum=args.vacuum,
        )


if __name__ == "__main__":
    main()
