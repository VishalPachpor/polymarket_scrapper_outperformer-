from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.enrichment.compute_path_metrics import (
    PathMetricsResult,
    compute_position_path_metrics,
)
from tip_v1.enrichment.fetch_price_history import (
    PriceHistoryFetchResult,
    fetch_wallet_price_history,
)


@dataclass(frozen=True)
class PathBackfillTarget:
    wallet: str
    closed_positions: int
    path_positions: int
    missing_positions: int
    rank_score: float


@dataclass(frozen=True)
class PathBackfillResult:
    wallets_considered: int
    wallets_processed: int
    positions_processed: int
    positions_with_path_data: int
    assets_requested: int
    assets_fetched: int
    assets_failed: int
    fetched_samples: int
    inserted_samples: int


def find_wallets_missing_path_metrics(
    *,
    settings: Settings | None = None,
    version: int = 1,
    min_trades: int = 3,
    limit: int = 20,
    wallet: str | None = None,
) -> list[PathBackfillTarget]:
    settings = settings or get_settings()
    wallet_clause = "AND p.wallet = ?" if wallet is not None else ""
    params: list[object] = [version]
    if wallet is not None:
        params.append(wallet)
    params.extend([min_trades, limit])

    with managed_connection(settings) as connection:
        rows = connection.execute(
            f"""
            WITH wallet_stats AS (
                SELECT
                    p.wallet AS wallet,
                    COUNT(*) AS closed_positions,
                    SUM(CASE WHEN pm.position_id IS NOT NULL THEN 1 ELSE 0 END) AS path_positions
                FROM positions_reconstructed AS p
                LEFT JOIN position_path_metrics AS pm
                  ON pm.position_id = p.id
                WHERE p.version = ?
                  AND p.status = 'CLOSED'
                  {wallet_clause}
                GROUP BY p.wallet
            )
            SELECT
                ws.wallet,
                ws.closed_positions,
                ws.path_positions,
                ws.closed_positions - ws.path_positions AS missing_positions,
                COALESCE(wr.rank_score, 0.0) AS rank_score
            FROM wallet_stats AS ws
            LEFT JOIN wallet_rankings AS wr
              ON wr.wallet = ws.wallet AND wr.regime = 'global'
            WHERE ws.closed_positions >= ?
              AND ws.path_positions < ws.closed_positions
            ORDER BY missing_positions DESC, rank_score DESC, ws.closed_positions DESC, ws.wallet
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    return [
        PathBackfillTarget(
            wallet=str(row["wallet"]),
            closed_positions=int(row["closed_positions"] or 0),
            path_positions=int(row["path_positions"] or 0),
            missing_positions=int(row["missing_positions"] or 0),
            rank_score=float(row["rank_score"] or 0.0),
        )
        for row in rows
    ]


def backfill_path_metrics(
    *,
    settings: Settings | None = None,
    version: int = 1,
    min_trades: int = 3,
    limit: int = 20,
    wallet: str | None = None,
    skip_price_fetch: bool = False,
) -> PathBackfillResult:
    settings = settings or get_settings()
    targets = find_wallets_missing_path_metrics(
        settings=settings,
        version=version,
        min_trades=min_trades,
        limit=limit,
        wallet=wallet,
    )

    wallets_processed = 0
    positions_processed = 0
    positions_with_path_data = 0
    assets_requested = 0
    assets_fetched = 0
    assets_failed = 0
    fetched_samples = 0
    inserted_samples = 0

    for target in targets:
        if not skip_price_fetch:
            fetch_result: PriceHistoryFetchResult = fetch_wallet_price_history(
                target.wallet,
                settings=settings,
            )
            assets_requested += fetch_result.assets_requested
            assets_fetched += fetch_result.assets_fetched
            assets_failed += fetch_result.assets_failed
            fetched_samples += fetch_result.fetched_samples
            inserted_samples += fetch_result.inserted_samples

        path_result: PathMetricsResult = compute_position_path_metrics(
            target.wallet,
            settings=settings,
            version=version,
        )
        wallets_processed += 1
        positions_processed += path_result.positions_processed
        positions_with_path_data += path_result.positions_with_path_data

    return PathBackfillResult(
        wallets_considered=len(targets),
        wallets_processed=wallets_processed,
        positions_processed=positions_processed,
        positions_with_path_data=positions_with_path_data,
        assets_requested=assets_requested,
        assets_fetched=assets_fetched,
        assets_failed=assets_failed,
        fetched_samples=fetched_samples,
        inserted_samples=inserted_samples,
    )


def build_backfill_report(result: PathBackfillResult, *, version: int, min_trades: int, skip_price_fetch: bool) -> str:
    lines = [
        f"Path Metrics Backfill | version={version} | min_trades={min_trades} | skip_price_fetch={skip_price_fetch}",
        "",
        f"Wallets considered: {result.wallets_considered}",
        f"Wallets processed: {result.wallets_processed}",
        f"Positions processed: {result.positions_processed}",
        f"Positions with path data: {result.positions_with_path_data}",
        f"Assets requested: {result.assets_requested}",
        f"Assets fetched: {result.assets_fetched}",
        f"Assets failed: {result.assets_failed}",
        f"Fetched samples: {result.fetched_samples}",
        f"Inserted samples: {result.inserted_samples}",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing position-path metrics for wallets with reconstructed closed positions.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--wallet", help="Optional single wallet to backfill.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--min-trades", type=int, default=3, help="Minimum closed positions per wallet.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum wallets to process in one run.")
    parser.add_argument(
        "--skip-price-fetch",
        action="store_true",
        help="Skip price-history fetching and recompute path metrics using already stored market history.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)
    result = backfill_path_metrics(
        settings=settings,
        version=args.version,
        min_trades=args.min_trades,
        limit=args.limit,
        wallet=args.wallet,
        skip_price_fetch=args.skip_price_fetch,
    )
    print(
        build_backfill_report(
            result,
            version=args.version,
            min_trades=args.min_trades,
            skip_price_fetch=args.skip_price_fetch,
        )
    )


if __name__ == "__main__":
    main()
