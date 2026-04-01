from __future__ import annotations

import statistics
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class MicrostructureFeatureResult:
    snapshots_processed: int
    features_computed: int


def _price_change(
    current_price: float | None,
    prior_price: float | None,
) -> float | None:
    if current_price in (None, 0) or prior_price in (None, 0):
        return None
    return (current_price - prior_price) / prior_price


def _volatility(prices: list[float]) -> float | None:
    if len(prices) < 3:
        return None

    returns: list[float] = []
    for left, right in zip(prices, prices[1:]):
        if left > 0:
            returns.append((right - left) / left)

    if len(returns) < 2:
        return None

    return statistics.pstdev(returns)


def compute_microstructure_features(
    settings: Settings | None = None,
    *,
    batch_size: int = 500,
) -> MicrostructureFeatureResult:
    settings = settings or get_settings()
    snapshots_processed = 0
    features_computed = 0

    with managed_connection(settings) as connection:
        while True:
            rows = connection.execute(
                """
                SELECT snapshot.id, snapshot.asset_id, snapshot.timestamp, snapshot.mid_price
                FROM microstructure_book_snapshots AS snapshot
                LEFT JOIN microstructure_features AS features
                    ON features.snapshot_id = snapshot.id
                WHERE features.snapshot_id IS NULL
                ORDER BY snapshot.asset_id, snapshot.timestamp, snapshot.id
                LIMIT ?
                """,
                (batch_size,),
            ).fetchall()
            if not rows:
                break

            for row in rows:
                snapshots_processed += 1
                snapshot_id = int(row["id"])
                asset_id = str(row["asset_id"])
                timestamp = int(row["timestamp"])
                current_mid = float(row["mid_price"]) if row["mid_price"] is not None else None

                prior_5s = connection.execute(
                    """
                    SELECT mid_price
                    FROM microstructure_book_snapshots
                    WHERE asset_id = ? AND timestamp <= ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT 1
                    """,
                    (asset_id, timestamp - 5),
                ).fetchone()
                prior_30s = connection.execute(
                    """
                    SELECT mid_price
                    FROM microstructure_book_snapshots
                    WHERE asset_id = ? AND timestamp <= ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT 1
                    """,
                    (asset_id, timestamp - 30),
                ).fetchone()
                prior_5m = connection.execute(
                    """
                    SELECT mid_price
                    FROM microstructure_book_snapshots
                    WHERE asset_id = ? AND timestamp <= ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT 1
                    """,
                    (asset_id, timestamp - 300),
                ).fetchone()
                prices_30s = [
                    float(value["mid_price"])
                    for value in connection.execute(
                        """
                        SELECT mid_price
                        FROM microstructure_book_snapshots
                        WHERE asset_id = ? AND timestamp BETWEEN ? AND ? AND mid_price IS NOT NULL
                        ORDER BY timestamp, id
                        """,
                        (asset_id, timestamp - 30, timestamp),
                    ).fetchall()
                ]
                prices_5m = [
                    float(value["mid_price"])
                    for value in connection.execute(
                        """
                        SELECT mid_price
                        FROM microstructure_book_snapshots
                        WHERE asset_id = ? AND timestamp BETWEEN ? AND ? AND mid_price IS NOT NULL
                        ORDER BY timestamp, id
                        """,
                        (asset_id, timestamp - 300, timestamp),
                    ).fetchall()
                ]
                snapshots_last_minute = connection.execute(
                    """
                    SELECT COUNT(*) AS count_last_minute
                    FROM microstructure_book_snapshots
                    WHERE asset_id = ? AND timestamp BETWEEN ? AND ?
                    """,
                    (asset_id, timestamp - 60, timestamp),
                ).fetchone()

                cursor = connection.execute(
                    """
                    INSERT INTO microstructure_features (
                        snapshot_id,
                        asset_id,
                        timestamp,
                        price_change_5s,
                        price_change_30s,
                        price_change_5m,
                        volatility_30s,
                        volatility_5m,
                        snapshots_per_minute
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        asset_id,
                        timestamp,
                        _price_change(
                            current_mid,
                            float(prior_5s["mid_price"]) if prior_5s and prior_5s["mid_price"] is not None else None,
                        ),
                        _price_change(
                            current_mid,
                            float(prior_30s["mid_price"]) if prior_30s and prior_30s["mid_price"] is not None else None,
                        ),
                        _price_change(
                            current_mid,
                            float(prior_5m["mid_price"]) if prior_5m and prior_5m["mid_price"] is not None else None,
                        ),
                        _volatility(prices_30s),
                        _volatility(prices_5m),
                        float(snapshots_last_minute["count_last_minute"] or 0),
                    ),
                )
                features_computed += cursor.rowcount

            connection.commit()

    return MicrostructureFeatureResult(
        snapshots_processed=snapshots_processed,
        features_computed=features_computed,
    )
