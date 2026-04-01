from __future__ import annotations

from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class MetricsResult:
    wallet: str
    total_positions: int
    win_count: int
    loss_count: int
    breakeven_count: int
    win_rate: float
    avg_pnl: float
    avg_hold_time: float


def compute_metrics(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
) -> MetricsResult:
    settings = settings or get_settings()

    with managed_connection(settings) as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS total_positions,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS win_count,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS loss_count,
                SUM(CASE WHEN pnl = 0 THEN 1 ELSE 0 END) AS breakeven_count,
                AVG(pnl) AS avg_pnl,
                AVG(duration) AS avg_hold_time
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ? AND status = 'CLOSED'
            """,
            (wallet, version),
        ).fetchone()

        total = int(row["total_positions"] or 0)
        win_count = int(row["win_count"] or 0)
        loss_count = int(row["loss_count"] or 0)
        breakeven_count = int(row["breakeven_count"] or 0)
        decisive = win_count + loss_count
        win_rate = (win_count / decisive) if decisive > 0 else 0.0

        result = MetricsResult(
            wallet=wallet,
            total_positions=total,
            win_count=win_count,
            loss_count=loss_count,
            breakeven_count=breakeven_count,
            win_rate=win_rate,
            avg_pnl=float(row["avg_pnl"] or 0.0),
            avg_hold_time=float(row["avg_hold_time"] or 0.0),
        )

        connection.execute(
            """
            INSERT INTO trader_metrics_summary (
                wallet,
                total_positions,
                win_count,
                loss_count,
                breakeven_count,
                win_rate,
                avg_pnl,
                avg_hold_time,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(wallet) DO UPDATE SET
                total_positions = excluded.total_positions,
                win_count = excluded.win_count,
                loss_count = excluded.loss_count,
                breakeven_count = excluded.breakeven_count,
                win_rate = excluded.win_rate,
                avg_pnl = excluded.avg_pnl,
                avg_hold_time = excluded.avg_hold_time,
                updated_at = excluded.updated_at
            """,
            (
                result.wallet,
                result.total_positions,
                result.win_count,
                result.loss_count,
                result.breakeven_count,
                result.win_rate,
                result.avg_pnl,
                result.avg_hold_time,
            ),
        )
        connection.commit()

    return result
