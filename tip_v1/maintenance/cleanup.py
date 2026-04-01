from __future__ import annotations

import time
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import get_connection, managed_connection


@dataclass(frozen=True)
class CleanupResult:
    raw_book_deleted: int
    price_history_deleted: int
    vacuum_run: bool


def cleanup_microstructure_raw(
    settings: Settings,
    *,
    max_age_seconds: int = 86_400 * 2,  # 48 hours
) -> int:
    """Delete old microstructure data in FK-safe dependency order.

    Deletion order:
      1. microstructure_features (child of book_snapshots)
      2. microstructure_book_snapshots — only rows NOT referenced by trade_entry_context
      3. microstructure_raw_book — orphaned rows with no remaining snapshot child

    Snapshots referenced by trade_entry_context are preserved so that
    already-joined trade context rows remain intact.
    """
    cutoff = int(time.time()) - max_age_seconds
    deleted_total = 0

    with managed_connection(settings) as connection:
        # 1. Delete features for old snapshots not referenced by trade_entry_context
        connection.execute(
            """
            DELETE FROM microstructure_features
            WHERE snapshot_id IN (
                SELECT id FROM microstructure_book_snapshots
                WHERE timestamp < ?
                  AND id NOT IN (
                      SELECT snapshot_id FROM trade_entry_context
                      WHERE snapshot_id IS NOT NULL
                  )
            )
            """,
            (cutoff,),
        )

        # 2. Delete old snapshots not referenced by trade_entry_context
        cursor = connection.execute(
            """
            DELETE FROM microstructure_book_snapshots
            WHERE timestamp < ?
              AND id NOT IN (
                  SELECT snapshot_id FROM trade_entry_context
                  WHERE snapshot_id IS NOT NULL
              )
            """,
            (cutoff,),
        )
        deleted_total += cursor.rowcount

        # 3. Delete raw_book rows that have no remaining snapshot child
        cursor = connection.execute(
            """
            DELETE FROM microstructure_raw_book
            WHERE observed_at < ?
              AND id NOT IN (
                  SELECT raw_book_id FROM microstructure_book_snapshots
              )
            """,
            (cutoff,),
        )
        deleted_total += cursor.rowcount

        connection.commit()

    return deleted_total


def cleanup_price_history(
    settings: Settings,
    *,
    max_age_seconds: int = 86_400 * 7,  # 7 days
) -> int:
    """Delete market_price_history rows older than max_age_seconds.

    Only deletes rows where the corresponding minute bucket has already
    been aggregated into price_history_agg_1m to prevent signal loss.
    """
    cutoff = int(time.time()) - max_age_seconds
    with managed_connection(settings) as connection:
        cursor = connection.execute(
            """
            DELETE FROM market_price_history
            WHERE timestamp < ?
              AND EXISTS (
                  SELECT 1 FROM price_history_agg_1m
                  WHERE price_history_agg_1m.asset_id  = market_price_history.asset_id
                    AND price_history_agg_1m.minute_ts = market_price_history.timestamp / 60 * 60
              )
            """,
            (cutoff,),
        )
        deleted = cursor.rowcount
        connection.commit()
    return deleted


def run_vacuum(settings: Settings) -> None:
    """Run VACUUM to reclaim disk space after large deletes.

    VACUUM cannot run inside a transaction, so this uses a raw connection
    in autocommit mode.
    """
    connection = get_connection(settings)
    try:
        connection.isolation_level = None  # autocommit — required for VACUUM
        connection.execute("VACUUM")
    finally:
        connection.close()


def run_cleanup(
    settings: Settings | None = None,
    *,
    micro_raw_max_age_seconds: int = 86_400 * 2,
    price_history_max_age_seconds: int = 86_400 * 7,
    vacuum: bool = False,
) -> CleanupResult:
    settings = settings or get_settings()
    raw_deleted = cleanup_microstructure_raw(
        settings, max_age_seconds=micro_raw_max_age_seconds
    )
    price_deleted = cleanup_price_history(
        settings, max_age_seconds=price_history_max_age_seconds
    )
    if vacuum:
        run_vacuum(settings)
    return CleanupResult(
        raw_book_deleted=raw_deleted,
        price_history_deleted=price_deleted,
        vacuum_run=vacuum,
    )
