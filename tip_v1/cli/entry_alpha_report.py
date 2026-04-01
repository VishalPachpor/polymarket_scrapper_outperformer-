from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection


@dataclass(frozen=True)
class WalletEntryAlphaMetrics:
    wallet: str
    trades: int
    win_rate: float
    avg_pnl: float
    avg_mfe: float
    avg_mae: float
    avg_mfe_return: float
    entry_alpha_rate: float
    avg_capture_ratio: float
    avg_giveback_ratio: float
    avg_time_to_mfe: float
    avg_time_after_mfe: float
    execution_leak_rate: float


def load_wallet_entry_alpha_metrics(
    *,
    settings: Settings | None = None,
    version: int = 1,
    wallet: str | None = None,
    min_trades: int = 5,
    top_n: int = 20,
    mfe_return_threshold: float = 0.05,
    weak_capture_threshold: float = 0.50,
) -> list[WalletEntryAlphaMetrics]:
    settings = settings or get_settings()

    where_wallet = "AND p.wallet = ?" if wallet is not None else ""
    params: list[object] = [version, mfe_return_threshold, mfe_return_threshold, weak_capture_threshold, min_trades, top_n]
    if wallet is not None:
        params = [version, wallet, mfe_return_threshold, mfe_return_threshold, weak_capture_threshold, min_trades, top_n]

    with managed_connection(settings) as connection:
        rows = connection.execute(
            f"""
            WITH position_metrics AS (
                SELECT
                    p.wallet AS wallet,
                    p.pnl AS pnl,
                    p.entry_price AS entry_price,
                    p.size AS size,
                    p.duration AS duration,
                    pm.mfe AS mfe,
                    pm.mae AS mae,
                    pm.time_to_mfe AS time_to_mfe,
                    CASE
                        WHEN p.entry_price IS NOT NULL AND p.entry_price > 0
                         AND p.size IS NOT NULL AND p.size > 0
                         AND pm.mfe IS NOT NULL
                        THEN pm.mfe / (p.entry_price * p.size)
                        ELSE NULL
                    END AS mfe_return,
                    CASE
                        WHEN pm.mfe IS NOT NULL AND pm.mfe > 0 AND p.pnl IS NOT NULL
                        THEN p.pnl / pm.mfe
                        ELSE NULL
                    END AS capture_ratio,
                    CASE
                        WHEN pm.mfe IS NOT NULL AND pm.mfe > 0 AND p.pnl IS NOT NULL
                        THEN MAX(0.0, (pm.mfe - p.pnl) / pm.mfe)
                        ELSE NULL
                    END AS giveback_ratio,
                    CASE
                        WHEN p.duration IS NOT NULL AND pm.time_to_mfe IS NOT NULL
                        THEN MAX(0, p.duration - pm.time_to_mfe)
                        ELSE NULL
                    END AS time_after_mfe
                FROM positions_reconstructed AS p
                JOIN position_path_metrics AS pm
                  ON pm.position_id = p.id
                WHERE p.version = ?
                  AND p.status = 'CLOSED'
                  AND p.pnl IS NOT NULL
                  {where_wallet}
            )
            SELECT
                wallet,
                COUNT(*) AS trades,
                AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
                AVG(pnl) AS avg_pnl,
                AVG(mfe) AS avg_mfe,
                AVG(mae) AS avg_mae,
                AVG(mfe_return) AS avg_mfe_return,
                AVG(CASE WHEN mfe_return >= ? THEN 1.0 ELSE 0.0 END) AS entry_alpha_rate,
                AVG(capture_ratio) AS avg_capture_ratio,
                AVG(giveback_ratio) AS avg_giveback_ratio,
                AVG(time_to_mfe) AS avg_time_to_mfe,
                AVG(time_after_mfe) AS avg_time_after_mfe,
                AVG(
                    CASE
                        WHEN mfe_return >= ? AND capture_ratio IS NOT NULL AND capture_ratio < ?
                        THEN 1.0
                        ELSE 0.0
                    END
                ) AS execution_leak_rate
            FROM position_metrics
            GROUP BY wallet
            HAVING COUNT(*) >= ?
            ORDER BY entry_alpha_rate DESC, avg_mfe_return DESC, avg_capture_ratio ASC, trades DESC, wallet
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    return [
        WalletEntryAlphaMetrics(
            wallet=str(row["wallet"]),
            trades=int(row["trades"] or 0),
            win_rate=float(row["win_rate"] or 0.0),
            avg_pnl=float(row["avg_pnl"] or 0.0),
            avg_mfe=float(row["avg_mfe"] or 0.0),
            avg_mae=float(row["avg_mae"] or 0.0),
            avg_mfe_return=float(row["avg_mfe_return"] or 0.0),
            entry_alpha_rate=float(row["entry_alpha_rate"] or 0.0),
            avg_capture_ratio=float(row["avg_capture_ratio"] or 0.0),
            avg_giveback_ratio=float(row["avg_giveback_ratio"] or 0.0),
            avg_time_to_mfe=float(row["avg_time_to_mfe"] or 0.0),
            avg_time_after_mfe=float(row["avg_time_after_mfe"] or 0.0),
            execution_leak_rate=float(row["execution_leak_rate"] or 0.0),
        )
        for row in rows
    ]


def build_entry_alpha_report(
    *,
    settings: Settings | None = None,
    version: int = 1,
    wallet: str | None = None,
    min_trades: int = 5,
    top_n: int = 20,
    mfe_return_threshold: float = 0.05,
    weak_capture_threshold: float = 0.50,
) -> str:
    metrics = load_wallet_entry_alpha_metrics(
        settings=settings,
        version=version,
        wallet=wallet,
        min_trades=min_trades,
        top_n=top_n,
        mfe_return_threshold=mfe_return_threshold,
        weak_capture_threshold=weak_capture_threshold,
    )

    if not metrics:
        target = wallet or "wallets"
        return f"Entry Alpha Report: {target}\nNo qualifying positions found."

    if wallet is not None:
        row = metrics[0]
        lines = [
            f"Entry Alpha Report: {row.wallet}",
            f"Closed Trades: {row.trades}",
            f"Win Rate: {row.win_rate:.2%}",
            f"Avg PnL: {row.avg_pnl:+.4f}",
            f"Avg MFE: {row.avg_mfe:+.4f}",
            f"Avg MAE: {row.avg_mae:+.4f}",
            f"Avg MFE Return: {row.avg_mfe_return:.2%}",
            f"Entry Alpha Rate (>={mfe_return_threshold:.0%} MFE): {row.entry_alpha_rate:.2%}",
            f"Capture Ratio: {row.avg_capture_ratio:.2f}",
            f"Giveback Ratio: {row.avg_giveback_ratio:.2%}",
            f"Execution Leak Rate: {row.execution_leak_rate:.2%}",
            f"Avg Time to MFE: {row.avg_time_to_mfe:.0f}s",
            f"Avg Time After MFE: {row.avg_time_after_mfe:.0f}s",
        ]
        return "\n".join(lines)

    lines = [
        (
            "Wallet Entry Alpha Leaderboard | "
            f"min_trades={min_trades} | entry_alpha_threshold={mfe_return_threshold:.0%} | "
            f"weak_capture_threshold={weak_capture_threshold:.0%}"
        ),
        "",
    ]
    for index, row in enumerate(metrics, start=1):
        lines.append(
            f"{index:>2}. {row.wallet} | trades={row.trades} | entry_alpha={row.entry_alpha_rate:.2%} | "
            f"avg_mfe_ret={row.avg_mfe_return:.2%} | capture={row.avg_capture_ratio:.2f} | "
            f"giveback={row.avg_giveback_ratio:.2%} | leak={row.execution_leak_rate:.2%}"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report wallet entry alpha and exit quality from reconstructed positions.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--wallet", help="Optional wallet to report in detail.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--min-trades", type=int, default=5, help="Minimum closed trades per wallet.")
    parser.add_argument("--top", type=int, default=20, help="Number of wallets to show in leaderboard mode.")
    parser.add_argument(
        "--mfe-threshold",
        type=float,
        default=0.05,
        help="Entry alpha threshold as normalized MFE return (default: 0.05 for 5%%).",
    )
    parser.add_argument(
        "--weak-capture-threshold",
        type=float,
        default=0.50,
        help="Capture-ratio threshold below which good entries are treated as execution leaks.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)
    print(
        build_entry_alpha_report(
            settings=settings,
            version=args.version,
            wallet=args.wallet,
            min_trades=args.min_trades,
            top_n=args.top,
            mfe_return_threshold=args.mfe_threshold,
            weak_capture_threshold=args.weak_capture_threshold,
        )
    )


if __name__ == "__main__":
    main()
