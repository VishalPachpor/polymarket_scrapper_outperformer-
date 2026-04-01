from __future__ import annotations

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


def build_report(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
    top_n: int = 5,
) -> str:
    settings = settings or get_settings()

    with managed_connection(settings) as connection:
        summary = connection.execute(
            """
            SELECT total_positions, win_count, loss_count, breakeven_count,
                   win_rate, avg_pnl, avg_hold_time, updated_at
            FROM trader_metrics_summary
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchone()

        market_rows = connection.execute(
            """
            SELECT
                market_id,
                outcome,
                COUNT(*) AS positions,
                SUM(pnl) AS total_pnl,
                AVG(pnl) AS avg_pnl
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ? AND status = 'CLOSED'
            GROUP BY market_id, outcome
            ORDER BY total_pnl DESC
            LIMIT ?
            """,
            (wallet, version, top_n),
        ).fetchall()

    if not summary:
        return f"Wallet: {wallet}\nNo metrics available."

    lines = [
        f"Wallet: {wallet}",
        f"Total Closed Positions: {summary['total_positions']}",
        f"Win/Loss/Breakeven: {summary['win_count']}/{summary['loss_count']}/{summary['breakeven_count']}",
        f"Win Rate: {summary['win_rate']:.2%} (excludes breakeven)",
        f"Avg PnL: {summary['avg_pnl']:.6f}",
        f"Avg Hold Time: {summary['avg_hold_time']:.2f}s",
        f"Updated At: {summary['updated_at']}",
        "",
        "Top Markets:",
    ]

    if not market_rows:
        lines.append("- No closed positions reconstructed yet")
        return "\n".join(lines)

    for row in market_rows:
        lines.append(
            f"- {row['market_id']} [{row['outcome']}] | positions={row['positions']} | "
            f"total_pnl={row['total_pnl']:.6f} | avg_pnl={row['avg_pnl']:.6f}"
        )

    return "\n".join(lines)


def print_report(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
    top_n: int = 5,
) -> None:
    print(build_report(wallet, settings=settings, version=version, top_n=top_n))
