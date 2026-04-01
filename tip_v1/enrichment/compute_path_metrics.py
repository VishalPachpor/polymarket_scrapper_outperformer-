from __future__ import annotations

from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class PathMetricsResult:
    wallet: str
    positions_processed: int
    positions_with_asset: int
    positions_with_path_data: int


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(float(value), 12)


def compute_position_path_metrics(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
) -> PathMetricsResult:
    settings = settings or get_settings()

    positions_processed = 0
    positions_with_asset = 0
    positions_with_path_data = 0

    with managed_connection(settings) as connection:
        connection.execute(
            """
            DELETE FROM position_path_metrics
            WHERE wallet = ? AND version = ?
            """,
            (wallet, version),
        )

        positions = connection.execute(
            """
            SELECT
                p.id,
                p.wallet,
                p.entry_price,
                p.exit_price,
                p.size,
                p.entry_time,
                p.exit_time,
                p.status,
                entry_event.asset_id AS asset_id
            FROM positions_reconstructed AS p
            JOIN trade_events AS entry_event
                ON entry_event.id = p.entry_trade_event_id
            WHERE p.wallet = ? AND p.version = ?
            ORDER BY p.id
            """,
            (wallet, version),
        ).fetchall()

        for position in positions:
            positions_processed += 1
            asset_id = position["asset_id"]
            if asset_id is None:
                continue

            positions_with_asset += 1
            entry_time = int(position["entry_time"])
            if position["status"] == "CLOSED" and position["exit_time"] is not None:
                end_time = int(position["exit_time"])
            else:
                row = connection.execute(
                    """
                    SELECT MAX(timestamp) AS last_timestamp
                    FROM market_price_history
                    WHERE asset_id = ? AND timestamp >= ?
                    """,
                    (asset_id, entry_time),
                ).fetchone()
                end_time = int(row["last_timestamp"]) if row["last_timestamp"] is not None else entry_time

            history_rows = connection.execute(
                """
                SELECT timestamp, price
                FROM market_price_history
                WHERE asset_id = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp, id
                """,
                (asset_id, entry_time, end_time),
            ).fetchall()

            samples = [(entry_time, float(position["entry_price"]))]
            samples.extend((int(row["timestamp"]), float(row["price"])) for row in history_rows)

            if position["status"] == "CLOSED" and position["exit_time"] is not None and position["exit_price"] is not None:
                samples.append((int(position["exit_time"]), float(position["exit_price"])))

            samples.sort(key=lambda item: item[0])

            if not samples:
                continue

            positions_with_path_data += 1
            entry_price = float(position["entry_price"])
            size = float(position["size"])
            path_pnls = [((price - entry_price) * size, timestamp, price) for timestamp, price in samples]
            max_pnl, max_timestamp, max_price = max(path_pnls, key=lambda item: (item[0], -item[1]))
            min_pnl, min_timestamp, min_price = min(path_pnls, key=lambda item: (item[0], item[1]))
            exit_context_price = samples[-1][1]

            connection.execute(
                """
                INSERT INTO position_path_metrics (
                    position_id,
                    wallet,
                    version,
                    asset_id,
                    sample_count,
                    start_timestamp,
                    end_timestamp,
                    entry_context_price,
                    exit_context_price,
                    max_price,
                    min_price,
                    mfe,
                    mae,
                    time_to_mfe,
                    time_to_mae
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(position["id"]),
                    wallet,
                    version,
                    str(asset_id),
                    len(samples),
                    entry_time,
                    end_time,
                    _rounded(entry_price),
                    _rounded(exit_context_price),
                    _rounded(max_price),
                    _rounded(min_price),
                    _rounded(max_pnl),
                    _rounded(min_pnl),
                    max_timestamp - entry_time,
                    min_timestamp - entry_time,
                ),
            )

        connection.commit()

    return PathMetricsResult(
        wallet=wallet,
        positions_processed=positions_processed,
        positions_with_asset=positions_with_asset,
        positions_with_path_data=positions_with_path_data,
    )
