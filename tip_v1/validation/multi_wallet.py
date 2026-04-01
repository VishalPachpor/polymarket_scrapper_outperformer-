from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.main import PipelineResult, run_pipeline
from tip_v1.validation.live_wallet import (
    StateSnapshot,
    ValidationReport,
    clear_wallet_state,
    snapshot_wallet_state,
    validate_inventory_conservation,
    run_live_validation,
)


@dataclass(frozen=True)
class StageTiming:
    name: str
    seconds: float


@dataclass(frozen=True)
class AggregateSnapshot:
    wallet_count: int
    raw_count: int
    normalized_raw_count: int
    normalization_error_count: int
    quarantine_count: int
    event_count: int
    closed_positions: int
    open_positions: int
    event_hash: str
    position_hash: str
    metrics_hash: str


@dataclass(frozen=True)
class WalletSnapshotComparison:
    wallet: str
    individual_snapshot: StateSnapshot
    combined_snapshot: StateSnapshot
    matches: bool


@dataclass(frozen=True)
class MarketIdentityConflict:
    market_id: str
    asset_id: str
    outcomes: tuple[str, ...]


@dataclass(frozen=True)
class IndividualWalletValidation:
    wallet: str
    db_path: Path
    report: ValidationReport
    duration_seconds: float


@dataclass(frozen=True)
class CombinedWalletRun:
    aggregate_snapshot: AggregateSnapshot
    wallet_snapshots: dict[str, StateSnapshot]
    wallet_pipelines: dict[str, PipelineResult]
    market_identity_conflicts: list[MarketIdentityConflict]
    duration_seconds: float


@dataclass(frozen=True)
class MultiWalletValidationReport:
    wallets: tuple[str, ...]
    individual_validations: tuple[IndividualWalletValidation, ...]
    combined_baseline: CombinedWalletRun
    combined_rebuild: CombinedWalletRun
    wallet_comparisons: tuple[WalletSnapshotComparison, ...]
    union_matches_combined: bool
    combined_rebuild_matches_baseline: bool
    timings: tuple[StageTiming, ...]


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 12)


def _hash_rows(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _wallet_suffix(wallet: str) -> str:
    digest = hashlib.sha1(wallet.lower().encode("utf-8")).hexdigest()
    return digest[:10]


def wallet_db_path(base_path: Path, wallet: str) -> Path:
    return base_path.with_name(f"{base_path.stem}_{_wallet_suffix(wallet)}{base_path.suffix}")


def snapshot_multi_wallet_state(
    wallets: list[str] | tuple[str, ...],
    settings: Settings,
    *,
    version: int = 1,
) -> AggregateSnapshot:
    wallet_params = tuple(wallets)
    placeholders = ", ".join("?" for _ in wallet_params)

    with managed_connection(settings) as connection:
        raw_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM trades_raw WHERE wallet IN ({placeholders})",
                wallet_params,
            ).fetchone()[0]
        )
        normalized_raw_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM trades_raw
                WHERE wallet IN ({placeholders}) AND normalized_at IS NOT NULL
                """,
                wallet_params,
            ).fetchone()[0]
        )
        normalization_error_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM trades_raw
                WHERE wallet IN ({placeholders}) AND normalization_error IS NOT NULL
                """,
                wallet_params,
            ).fetchone()[0]
        )
        quarantine_count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM quarantined_trades WHERE wallet IN ({placeholders})",
                wallet_params,
            ).fetchone()[0]
        )

        event_rows = [
            dict(row)
            for row in connection.execute(
                f"""
                SELECT trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                FROM trade_events
                WHERE wallet IN ({placeholders})
                ORDER BY wallet, timestamp, trade_id, id
                """,
                wallet_params,
            ).fetchall()
        ]
        position_rows = [
            dict(row)
            for row in connection.execute(
                f"""
                SELECT
                    positions_reconstructed.wallet AS wallet,
                    positions_reconstructed.market_id AS market_id,
                    positions_reconstructed.outcome AS outcome,
                    entry_event.trade_id AS entry_trade_id,
                    exit_event.trade_id AS exit_trade_id,
                    positions_reconstructed.entry_price AS entry_price,
                    positions_reconstructed.exit_price AS exit_price,
                    positions_reconstructed.size AS size,
                    positions_reconstructed.pnl AS pnl,
                    positions_reconstructed.entry_time AS entry_time,
                    positions_reconstructed.exit_time AS exit_time,
                    positions_reconstructed.duration AS duration,
                    positions_reconstructed.status AS status,
                    positions_reconstructed.remaining_size AS remaining_size,
                    positions_reconstructed.version AS version
                FROM positions_reconstructed
                JOIN trade_events AS entry_event
                    ON entry_event.id = positions_reconstructed.entry_trade_event_id
                LEFT JOIN trade_events AS exit_event
                    ON exit_event.id = positions_reconstructed.exit_trade_event_id
                WHERE positions_reconstructed.wallet IN ({placeholders})
                  AND positions_reconstructed.version = ?
                ORDER BY
                    positions_reconstructed.wallet,
                    positions_reconstructed.market_id,
                    positions_reconstructed.outcome,
                    positions_reconstructed.entry_time,
                    COALESCE(positions_reconstructed.exit_time, 0),
                    positions_reconstructed.id
                """,
                wallet_params + (version,),
            ).fetchall()
        ]
        metrics_rows = [
            dict(row)
            for row in connection.execute(
                f"""
                SELECT wallet, total_positions, win_count, loss_count, breakeven_count, win_rate, avg_pnl, avg_hold_time
                FROM trader_metrics_summary
                WHERE wallet IN ({placeholders})
                ORDER BY wallet
                """,
                wallet_params,
            ).fetchall()
        ]

    for row in position_rows:
        for key in ("entry_price", "exit_price", "size", "pnl", "remaining_size"):
            row[key] = _rounded(row[key])

    for row in metrics_rows:
        for key in ("win_rate", "avg_pnl", "avg_hold_time"):
            row[key] = _rounded(row[key])

    return AggregateSnapshot(
        wallet_count=len(wallets),
        raw_count=raw_count,
        normalized_raw_count=normalized_raw_count,
        normalization_error_count=normalization_error_count,
        quarantine_count=quarantine_count,
        event_count=len(event_rows),
        closed_positions=sum(1 for row in position_rows if row["status"] == "CLOSED"),
        open_positions=sum(1 for row in position_rows if row["status"] == "OPEN"),
        event_hash=_hash_rows(event_rows),
        position_hash=_hash_rows(position_rows),
        metrics_hash=_hash_rows(metrics_rows),
    )


def validate_market_identity_consistency(
    wallets: list[str] | tuple[str, ...],
    settings: Settings,
) -> list[MarketIdentityConflict]:
    wallet_params = tuple(wallets)
    placeholders = ", ".join("?" for _ in wallet_params)

    with managed_connection(settings) as connection:
        rows = connection.execute(
            f"""
            SELECT
                market_id,
                COALESCE(asset_id, '') AS asset_id,
                GROUP_CONCAT(DISTINCT outcome) AS outcome_list,
                COUNT(DISTINCT outcome) AS outcome_count
            FROM trade_events
            WHERE wallet IN ({placeholders})
            GROUP BY market_id, COALESCE(asset_id, '')
            HAVING COUNT(DISTINCT outcome) > 1
            ORDER BY market_id, asset_id
            """,
            wallet_params,
        ).fetchall()

    conflicts: list[MarketIdentityConflict] = []
    for row in rows:
        outcomes = tuple(sorted(str(row["outcome_list"]).split(",")))
        conflicts.append(
            MarketIdentityConflict(
                market_id=str(row["market_id"]),
                asset_id=str(row["asset_id"]),
                outcomes=outcomes,
            )
        )
    return conflicts


def import_wallet_raw_data(
    wallet: str,
    source_settings: Settings,
    destination_settings: Settings,
) -> tuple[int, int]:
    inserted = 0
    duplicates = 0

    with managed_connection(source_settings) as source_connection:
        rows = source_connection.execute(
            """
            SELECT wallet, raw_json, fetched_at, dedupe_key
            FROM trades_raw
            WHERE wallet = ?
            ORDER BY id
            """,
            (wallet,),
        ).fetchall()

    with managed_connection(destination_settings) as destination_connection:
        for row in rows:
            cursor = destination_connection.execute(
                """
                INSERT OR IGNORE INTO trades_raw (wallet, raw_json, fetched_at, dedupe_key)
                VALUES (?, ?, ?, ?)
                """,
                (
                    row["wallet"],
                    row["raw_json"],
                    row["fetched_at"],
                    row["dedupe_key"],
                ),
            )
            inserted += cursor.rowcount
            duplicates += 1 - cursor.rowcount
        destination_connection.commit()

    return inserted, duplicates


def run_combined_wallet_build(
    wallets: list[str] | tuple[str, ...],
    settings: Settings,
    *,
    version: int = 1,
    fail_fast: bool = True,
) -> CombinedWalletRun:
    initialize_database(settings)
    wallet_pipelines: dict[str, PipelineResult] = {}
    wallet_snapshots: dict[str, StateSnapshot] = {}

    started = time.perf_counter()
    for wallet in wallets:
        pipeline = run_pipeline(
            wallet,
            settings=settings,
            skip_fetch=True,
            skip_enrichment=True,
            version=version,
            fail_on_unmatched_sells=fail_fast,
        )
        wallet_pipelines[wallet] = pipeline
        wallet_snapshots[wallet] = snapshot_wallet_state(wallet, settings, version=version)

        if fail_fast:
            violations = validate_inventory_conservation(wallet, settings, version=version)
            if violations:
                raise RuntimeError(
                    f"Combined build failed inventory conservation for wallet={wallet}."
                )
            snapshot = wallet_snapshots[wallet]
            if snapshot.normalization_error_count or snapshot.quarantine_count:
                raise RuntimeError(
                    f"Combined build failed due to quarantined or malformed rows for wallet={wallet}."
                )

    duration_seconds = time.perf_counter() - started
    aggregate_snapshot = snapshot_multi_wallet_state(wallets, settings, version=version)
    market_identity_conflicts = validate_market_identity_consistency(wallets, settings)
    if fail_fast and market_identity_conflicts:
        raise RuntimeError(
            f"Combined build found {len(market_identity_conflicts)} market identity conflicts."
        )

    return CombinedWalletRun(
        aggregate_snapshot=aggregate_snapshot,
        wallet_snapshots=wallet_snapshots,
        wallet_pipelines=wallet_pipelines,
        market_identity_conflicts=market_identity_conflicts,
        duration_seconds=duration_seconds,
    )


def run_multi_wallet_validation(
    wallets: list[str] | tuple[str, ...],
    settings: Settings,
    *,
    version: int = 1,
    page_limit: int | None = None,
    max_pages: int | None = None,
    pagination_small_limit: int | None = None,
    pagination_large_limit: int | None = None,
    pagination_record_cap: int = 200,
    fail_fast: bool = True,
    page_overlap: int | None = None,
    stabilization_sweeps: int = 3,
) -> MultiWalletValidationReport:
    unique_wallets = tuple(dict.fromkeys(wallets))
    if len(unique_wallets) < 2:
        raise ValueError("Multi-wallet validation requires at least two distinct wallets.")

    individual_validations: list[IndividualWalletValidation] = []
    timings: list[StageTiming] = []

    for wallet in unique_wallets:
        wallet_settings = get_settings(wallet_db_path(settings.database_path, wallet))
        if wallet_settings.database_path.exists():
            wallet_settings.database_path.unlink()

        started = time.perf_counter()
        report = run_live_validation(
            wallet,
            wallet_settings,
            version=version,
            page_limit=page_limit,
            max_pages=max_pages,
            pagination_small_limit=pagination_small_limit,
            pagination_large_limit=pagination_large_limit,
            pagination_record_cap=pagination_record_cap,
            fail_fast=fail_fast,
            page_overlap=page_overlap,
            stabilization_sweeps=stabilization_sweeps,
        )
        duration_seconds = time.perf_counter() - started
        individual_validations.append(
            IndividualWalletValidation(
                wallet=wallet,
                db_path=wallet_settings.database_path,
                report=report,
                duration_seconds=duration_seconds,
            )
        )
        timings.append(StageTiming(name=f"individual:{wallet}", seconds=duration_seconds))

    if settings.database_path.exists():
        settings.database_path.unlink()
    initialize_database(settings)

    import_started = time.perf_counter()
    for validation in individual_validations:
        import_wallet_raw_data(validation.wallet, get_settings(validation.db_path), settings)
    timings.append(
        StageTiming(name="combined_import", seconds=time.perf_counter() - import_started)
    )

    combined_baseline = run_combined_wallet_build(
        unique_wallets,
        settings,
        version=version,
        fail_fast=fail_fast,
    )
    timings.append(
        StageTiming(name="combined_baseline", seconds=combined_baseline.duration_seconds)
    )

    wallet_comparisons: list[WalletSnapshotComparison] = []
    for validation in individual_validations:
        individual_snapshot = validation.report.baseline.snapshot
        combined_snapshot = combined_baseline.wallet_snapshots[validation.wallet]
        wallet_comparisons.append(
            WalletSnapshotComparison(
                wallet=validation.wallet,
                individual_snapshot=individual_snapshot,
                combined_snapshot=combined_snapshot,
                matches=individual_snapshot == combined_snapshot,
            )
        )

    for wallet in unique_wallets:
        clear_wallet_state(wallet, settings, version=version, include_raw=False)

    combined_rebuild = run_combined_wallet_build(
        unique_wallets,
        settings,
        version=version,
        fail_fast=fail_fast,
    )
    timings.append(
        StageTiming(name="combined_rebuild", seconds=combined_rebuild.duration_seconds)
    )

    union_matches_combined = all(comparison.matches for comparison in wallet_comparisons)
    combined_rebuild_matches_baseline = (
        combined_baseline.aggregate_snapshot == combined_rebuild.aggregate_snapshot
    )

    return MultiWalletValidationReport(
        wallets=unique_wallets,
        individual_validations=tuple(individual_validations),
        combined_baseline=combined_baseline,
        combined_rebuild=combined_rebuild,
        wallet_comparisons=tuple(wallet_comparisons),
        union_matches_combined=union_matches_combined,
        combined_rebuild_matches_baseline=combined_rebuild_matches_baseline,
        timings=tuple(timings),
    )


def _format_snapshot(snapshot: AggregateSnapshot) -> str:
    return (
        f"wallets={snapshot.wallet_count} raw={snapshot.raw_count} "
        f"events={snapshot.event_count} closed={snapshot.closed_positions} "
        f"open={snapshot.open_positions} errors={snapshot.normalization_error_count} "
        f"quarantined={snapshot.quarantine_count}"
    )


def print_multi_wallet_report(
    report: MultiWalletValidationReport,
    *,
    verbose: bool = False,
) -> None:
    lines = [f"wallets={','.join(report.wallets)}"]

    for validation in report.individual_validations:
        lines.append(
            f"[INDIVIDUAL] wallet={validation.wallet} duration={validation.duration_seconds:.2f}s "
            f"rebuild_matches={validation.report.rebuild_matches_baseline} "
            f"rerun_matches={validation.report.rerun_matches_baseline_without_new_data}"
        )

    lines.append(f"[COMBINED_BASELINE] {_format_snapshot(report.combined_baseline.aggregate_snapshot)}")
    lines.append(f"[COMBINED_REBUILD] {_format_snapshot(report.combined_rebuild.aggregate_snapshot)}")
    lines.append(f"union_matches_combined={report.union_matches_combined}")
    lines.append(f"combined_rebuild_matches_baseline={report.combined_rebuild_matches_baseline}")
    lines.append(
        f"market_identity_conflicts={len(report.combined_baseline.market_identity_conflicts)}"
    )

    for timing in report.timings:
        lines.append(f"[TIMING] {timing.name}={timing.seconds:.2f}s")

    for comparison in report.wallet_comparisons:
        lines.append(
            f"[WALLET_COMPARE] wallet={comparison.wallet} matches={comparison.matches} "
            f"events={comparison.combined_snapshot.event_count} "
            f"open={comparison.combined_snapshot.open_positions} "
            f"closed={comparison.combined_snapshot.closed_positions}"
        )

    if verbose:
        for comparison in report.wallet_comparisons:
            lines.append(
                f"[WALLET_HASHES] wallet={comparison.wallet} "
                f"individual_events={comparison.individual_snapshot.event_hash} "
                f"combined_events={comparison.combined_snapshot.event_hash} "
                f"individual_positions={comparison.individual_snapshot.position_hash} "
                f"combined_positions={comparison.combined_snapshot.position_hash} "
                f"individual_metrics={comparison.individual_snapshot.metrics_hash} "
                f"combined_metrics={comparison.combined_snapshot.metrics_hash}"
            )
        for conflict in report.combined_baseline.market_identity_conflicts:
            lines.append(
                f"[MARKET_IDENTITY_CONFLICT] market_id={conflict.market_id} "
                f"asset_id={conflict.asset_id} outcomes={','.join(conflict.outcomes)}"
            )

    print("\n".join(lines))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run multi-wallet validation for TIP V1.")
    parser.add_argument(
        "--wallet",
        action="append",
        required=True,
        help="Repeat for each wallet to validate.",
    )
    parser.add_argument("--db-path", required=True, help="Combined SQLite path.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--page-limit", type=int, help="Main run fetch page size.")
    parser.add_argument("--max-pages", type=int, help="Main run fetch max pages.")
    parser.add_argument(
        "--pagination-small-limit",
        type=int,
        help="Optional small page size for per-wallet pagination validation.",
    )
    parser.add_argument(
        "--pagination-large-limit",
        type=int,
        help="Optional large page size for per-wallet pagination validation.",
    )
    parser.add_argument(
        "--pagination-record-cap",
        type=int,
        default=200,
        help="Target recent trade count for per-wallet pagination checks.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on quarantines, identity conflicts, or inventory violations.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed per-wallet hashes and market identity conflicts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    report = run_multi_wallet_validation(
        args.wallet,
        settings,
        version=args.version,
        page_limit=args.page_limit,
        max_pages=args.max_pages,
        pagination_small_limit=args.pagination_small_limit,
        pagination_large_limit=args.pagination_large_limit,
        pagination_record_cap=args.pagination_record_cap,
        fail_fast=args.fail_fast,
    )
    print_multi_wallet_report(report, verbose=args.verbose)


if __name__ == "__main__":
    main()
