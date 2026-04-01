from __future__ import annotations

import math
import time
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class AggregationResult:
    micro_minutes_inserted: int
    micro_minutes_skipped: int
    price_minutes_inserted: int
    price_minutes_skipped: int


def aggregate_microstructure(
    settings: Settings,
    *,
    older_than_seconds: int = 7_200,  # 2 hours — ensures minute buckets are complete
) -> tuple[int, int]:
    """Aggregate microstructure_book_snapshots into microstructure_agg_1m.

    Only processes snapshots older than `older_than_seconds` so that in-progress
    minute buckets are never partially aggregated.

    Returns (inserted, skipped).
    """
    cutoff = int(time.time()) - older_than_seconds
    inserted = 0
    skipped = 0

    with managed_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT
                asset_id,
                timestamp / 60 * 60             AS minute_ts,
                AVG(best_bid)                   AS avg_bid,
                AVG(best_ask)                   AS avg_ask,
                AVG(spread)                     AS spread_avg,
                AVG(spread * spread)            AS spread_sq_avg,
                AVG(mid_price)                  AS mid_price_avg,
                COUNT(*)                        AS snapshot_count
            FROM microstructure_book_snapshots
            WHERE timestamp < ?
              AND best_bid IS NOT NULL
              AND spread IS NOT NULL
            GROUP BY asset_id, minute_ts
            ORDER BY asset_id, minute_ts
            """,
            (cutoff,),
        ).fetchall()

        for row in rows:
            spread_avg = float(row["spread_avg"] or 0.0)
            spread_sq_avg = float(row["spread_sq_avg"] or 0.0)
            spread_std = math.sqrt(max(0.0, spread_sq_avg - spread_avg ** 2))

            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO microstructure_agg_1m (
                    asset_id, minute_ts,
                    avg_bid, avg_ask,
                    spread_avg, spread_std,
                    mid_price_avg, snapshot_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row["asset_id"]),
                    int(row["minute_ts"]),
                    row["avg_bid"],
                    row["avg_ask"],
                    spread_avg,
                    spread_std,
                    row["mid_price_avg"],
                    int(row["snapshot_count"]),
                ),
            )
            inserted += cursor.rowcount
            skipped += 1 - cursor.rowcount

        connection.commit()

    return inserted, skipped


def aggregate_price_history(
    settings: Settings,
    *,
    older_than_seconds: int = 7_200,
) -> tuple[int, int]:
    """Aggregate market_price_history into price_history_agg_1m (OHLC per minute).

    Returns (inserted, skipped).
    """
    cutoff = int(time.time()) - older_than_seconds
    inserted = 0
    skipped = 0

    with managed_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT
                ph.asset_id,
                ph.timestamp / 60 * 60   AS minute_ts,
                MIN(ph.price)            AS low_price,
                MAX(ph.price)            AS high_price,
                COUNT(*)                 AS sample_count,
                (
                    SELECT ph2.price
                    FROM market_price_history ph2
                    WHERE ph2.asset_id = ph.asset_id
                      AND ph2.timestamp / 60 * 60 = ph.timestamp / 60 * 60
                    ORDER BY ph2.timestamp ASC
                    LIMIT 1
                ) AS open_price,
                (
                    SELECT ph2.price
                    FROM market_price_history ph2
                    WHERE ph2.asset_id = ph.asset_id
                      AND ph2.timestamp / 60 * 60 = ph.timestamp / 60 * 60
                    ORDER BY ph2.timestamp DESC
                    LIMIT 1
                ) AS close_price
            FROM market_price_history ph
            WHERE ph.timestamp < ?
            GROUP BY ph.asset_id, ph.timestamp / 60 * 60
            ORDER BY ph.asset_id, minute_ts
            """,
            (cutoff,),
        ).fetchall()

        for row in rows:
            if row["open_price"] is None or row["close_price"] is None:
                continue
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO price_history_agg_1m (
                    asset_id, minute_ts,
                    open_price, high_price, low_price, close_price,
                    sample_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row["asset_id"]),
                    int(row["minute_ts"]),
                    float(row["open_price"]),
                    float(row["high_price"]),
                    float(row["low_price"]),
                    float(row["close_price"]),
                    int(row["sample_count"]),
                ),
            )
            inserted += cursor.rowcount
            skipped += 1 - cursor.rowcount

        connection.commit()

    return inserted, skipped


def run_aggregation(
    settings: Settings | None = None,
    *,
    older_than_seconds: int = 7_200,
) -> AggregationResult:
    settings = settings or get_settings()
    micro_ins, micro_skip = aggregate_microstructure(
        settings, older_than_seconds=older_than_seconds
    )
    price_ins, price_skip = aggregate_price_history(
        settings, older_than_seconds=older_than_seconds
    )
    return AggregationResult(
        micro_minutes_inserted=micro_ins,
        micro_minutes_skipped=micro_skip,
        price_minutes_inserted=price_ins,
        price_minutes_skipped=price_skip,
    )
