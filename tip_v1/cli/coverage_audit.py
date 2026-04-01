from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection


@dataclass(frozen=True)
class CoverageAuditResult:
    positions_total: int
    path_metrics_total: int
    positions_with_path: int
    path_coverage_ratio: float
    wallets_total: int
    wallets_with_closed_positions: int
    wallets_min_trades: int
    wallets_with_any_path: int
    wallets_min_trades_with_any_path: int
    wallets_full_path_min_trades: int
    wallets_report_usable: int


def run_coverage_audit(
    *,
    settings: Settings | None = None,
    version: int = 1,
    min_trades: int = 3,
) -> CoverageAuditResult:
    settings = settings or get_settings()

    with managed_connection(settings) as connection:
        positions_total = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM positions_reconstructed
                WHERE version = ? AND status = 'CLOSED'
                """,
                (version,),
            ).fetchone()[0]
            or 0
        )
        path_metrics_total = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM position_path_metrics
                WHERE version = ?
                """,
                (version,),
            ).fetchone()[0]
            or 0
        )
        positions_with_path = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM positions_reconstructed AS p
                JOIN position_path_metrics AS pm
                  ON pm.position_id = p.id
                WHERE p.version = ? AND p.status = 'CLOSED'
                """,
                (version,),
            ).fetchone()[0]
            or 0
        )
        row = connection.execute(
            """
            WITH wallet_stats AS (
                SELECT
                    p.wallet AS wallet,
                    COUNT(*) AS closed_positions,
                    SUM(CASE WHEN pm.position_id IS NOT NULL THEN 1 ELSE 0 END) AS path_positions
                FROM positions_reconstructed AS p
                LEFT JOIN position_path_metrics AS pm
                  ON pm.position_id = p.id
                WHERE p.version = ? AND p.status = 'CLOSED'
                GROUP BY p.wallet
            )
            SELECT
                COUNT(*) AS wallets_with_closed_positions,
                SUM(CASE WHEN closed_positions >= ? THEN 1 ELSE 0 END) AS wallets_min_trades,
                SUM(CASE WHEN path_positions > 0 THEN 1 ELSE 0 END) AS wallets_with_any_path,
                SUM(CASE WHEN closed_positions >= ? AND path_positions > 0 THEN 1 ELSE 0 END) AS wallets_min_trades_with_any_path,
                SUM(CASE WHEN closed_positions >= ? AND path_positions = closed_positions THEN 1 ELSE 0 END) AS wallets_full_path_min_trades,
                SUM(CASE WHEN closed_positions >= ? AND path_positions >= ? THEN 1 ELSE 0 END) AS wallets_report_usable
            FROM wallet_stats
            """,
            (version, min_trades, min_trades, min_trades, min_trades, min_trades),
        ).fetchone()
        wallets_total = int(
            connection.execute(
                """
                SELECT COUNT(DISTINCT wallet)
                FROM positions_reconstructed
                WHERE version = ?
                """,
                (version,),
            ).fetchone()[0]
            or 0
        )

    return CoverageAuditResult(
        positions_total=positions_total,
        path_metrics_total=path_metrics_total,
        positions_with_path=positions_with_path,
        path_coverage_ratio=(positions_with_path / positions_total) if positions_total else 0.0,
        wallets_total=wallets_total,
        wallets_with_closed_positions=int(row["wallets_with_closed_positions"] or 0),
        wallets_min_trades=int(row["wallets_min_trades"] or 0),
        wallets_with_any_path=int(row["wallets_with_any_path"] or 0),
        wallets_min_trades_with_any_path=int(row["wallets_min_trades_with_any_path"] or 0),
        wallets_full_path_min_trades=int(row["wallets_full_path_min_trades"] or 0),
        wallets_report_usable=int(row["wallets_report_usable"] or 0),
    )


def build_coverage_audit_report(
    *,
    settings: Settings | None = None,
    version: int = 1,
    min_trades: int = 3,
) -> str:
    result = run_coverage_audit(settings=settings, version=version, min_trades=min_trades)
    lines = [
        f"Coverage Audit | version={version} | min_trades={min_trades}",
        "",
        f"Closed positions total: {result.positions_total}",
        f"Path metrics rows: {result.path_metrics_total}",
        f"Closed positions with path metrics: {result.positions_with_path}",
        f"Path coverage ratio: {result.path_coverage_ratio:.2%}",
        "",
        f"Wallets total (all statuses): {result.wallets_total}",
        f"Wallets with closed positions: {result.wallets_with_closed_positions}",
        f"Wallets with >={min_trades} closed positions: {result.wallets_min_trades}",
        f"Wallets with any path metrics: {result.wallets_with_any_path}",
        f"Wallets with >={min_trades} closed positions and any path metrics: {result.wallets_min_trades_with_any_path}",
        f"Wallets with >={min_trades} closed positions and full path coverage: {result.wallets_full_path_min_trades}",
        f"Wallets report-usable (>={min_trades} path-backed closed positions): {result.wallets_report_usable}",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit reconstructed-position and path-metric coverage.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--min-trades", type=int, default=3, help="Minimum closed trades threshold for wallet usability.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)
    print(
        build_coverage_audit_report(
            settings=settings,
            version=args.version,
            min_trades=args.min_trades,
        )
    )


if __name__ == "__main__":
    main()
