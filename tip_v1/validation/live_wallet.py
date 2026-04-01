from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.ingestion.fetch_trades import _default_page_overlap, _page_step
from tip_v1.main import PipelineResult, run_pipeline


EPSILON = 1e-9


@dataclass(frozen=True)
class InventoryViolation:
    market_id: str
    outcome: str
    total_buys: float
    total_sells: float
    expected_remaining: float
    actual_remaining: float


@dataclass(frozen=True)
class StateSnapshot:
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
class ValidationStep:
    name: str
    pipeline: PipelineResult
    snapshot: StateSnapshot
    inventory_violations: list[InventoryViolation]


@dataclass(frozen=True)
class ValidationReport:
    wallet: str
    baseline: ValidationStep
    rerun: ValidationStep
    rebuild: ValidationStep
    rebuild_matches_baseline: bool
    rerun_matches_baseline_without_new_data: bool | None
    pagination_small: ValidationStep | None = None
    pagination_large: ValidationStep | None = None
    pagination_matches: bool | None = None


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 12)


def _hash_rows(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_recent_trade_snapshot(
    wallet: str,
    settings: Settings,
    *,
    top_n: int,
) -> tuple[int, str]:
    with managed_connection(settings) as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                FROM trade_events
                WHERE wallet = ?
                ORDER BY timestamp DESC, trade_id DESC, id DESC
                LIMIT ?
                """,
                (wallet, top_n),
            ).fetchall()
        ]

    return len(rows), _hash_rows(rows)


def snapshot_wallet_state(
    wallet: str,
    settings: Settings,
    *,
    version: int = 1,
) -> StateSnapshot:
    with managed_connection(settings) as connection:
        raw_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM trades_raw WHERE wallet = ?",
                (wallet,),
            ).fetchone()[0]
        )
        normalized_raw_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM trades_raw
                WHERE wallet = ? AND normalized_at IS NOT NULL
                """,
                (wallet,),
            ).fetchone()[0]
        )
        normalization_error_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM trades_raw
                WHERE wallet = ? AND normalization_error IS NOT NULL
                """,
                (wallet,),
            ).fetchone()[0]
        )
        quarantine_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM quarantined_trades
                WHERE wallet = ?
                """,
                (wallet,),
            ).fetchone()[0]
        )

        event_rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                FROM trade_events
                WHERE wallet = ?
                ORDER BY timestamp, trade_id, id
                """,
                (wallet,),
            ).fetchall()
        ]
        position_rows = [
            dict(row)
            for row in connection.execute(
                """
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
                WHERE positions_reconstructed.wallet = ? AND positions_reconstructed.version = ?
                ORDER BY
                    positions_reconstructed.market_id,
                    positions_reconstructed.outcome,
                    positions_reconstructed.entry_time,
                    COALESCE(positions_reconstructed.exit_time, 0),
                    positions_reconstructed.id
                """,
                (wallet, version),
            ).fetchall()
        ]
        metrics_rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT wallet, total_positions, win_count, loss_count, breakeven_count, win_rate, avg_pnl, avg_hold_time
                FROM trader_metrics_summary
                WHERE wallet = ?
                """,
                (wallet,),
            ).fetchall()
        ]

    for row in position_rows:
        for key in ("entry_price", "exit_price", "size", "pnl", "remaining_size"):
            row[key] = _rounded(row[key])

    for row in metrics_rows:
        for key in ("win_rate", "avg_pnl", "avg_hold_time"):
            row[key] = _rounded(row[key])

    return StateSnapshot(
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


def validate_inventory_conservation(
    wallet: str,
    settings: Settings,
    *,
    version: int = 1,
) -> list[InventoryViolation]:
    with managed_connection(settings) as connection:
        event_groups = connection.execute(
            """
            SELECT
                market_id,
                outcome,
                SUM(CASE WHEN side = 'BUY' THEN CAST(size AS REAL) ELSE 0 END) AS total_buys,
                SUM(CASE WHEN side = 'SELL' THEN CAST(size AS REAL) ELSE 0 END) AS total_sells
            FROM trade_events
            WHERE wallet = ?
            GROUP BY market_id, outcome
            ORDER BY market_id, outcome
            """,
            (wallet,),
        ).fetchall()
        open_groups = {
            (row["market_id"], row["outcome"]): float(row["remaining_inventory"] or 0.0)
            for row in connection.execute(
                """
                SELECT
                    market_id,
                    outcome,
                    SUM(remaining_size) AS remaining_inventory
                FROM positions_reconstructed
                WHERE wallet = ? AND version = ? AND status = 'OPEN'
                GROUP BY market_id, outcome
                """,
                (wallet, version),
            ).fetchall()
        }

    violations: list[InventoryViolation] = []
    for row in event_groups:
        total_buys = float(row["total_buys"] or 0.0)
        total_sells = float(row["total_sells"] or 0.0)
        expected_remaining = total_buys - total_sells
        actual_remaining = open_groups.get((row["market_id"], row["outcome"]), 0.0)

        if abs(expected_remaining - actual_remaining) > EPSILON:
            violations.append(
                InventoryViolation(
                    market_id=str(row["market_id"]),
                    outcome=str(row["outcome"]),
                    total_buys=round(total_buys, 12),
                    total_sells=round(total_sells, 12),
                    expected_remaining=round(expected_remaining, 12),
                    actual_remaining=round(actual_remaining, 12),
                )
            )

    return violations


def clear_wallet_state(
    wallet: str,
    settings: Settings,
    *,
    version: int = 1,
    include_raw: bool = False,
    reset_raw_normalization: bool = True,
) -> None:
    with managed_connection(settings) as connection:
        connection.execute(
            "DELETE FROM trader_metrics_summary WHERE wallet = ?",
            (wallet,),
        )
        connection.execute(
            "DELETE FROM positions_reconstructed WHERE wallet = ? AND version = ?",
            (wallet, version),
        )
        connection.execute(
            "DELETE FROM trade_events WHERE wallet = ?",
            (wallet,),
        )
        if reset_raw_normalization:
            connection.execute(
                """
                UPDATE trades_raw
                SET normalized_at = NULL, normalization_error = NULL
                WHERE wallet = ?
                """,
                (wallet,),
            )
            connection.execute(
                "DELETE FROM quarantined_trades WHERE wallet = ?",
                (wallet,),
            )
        if include_raw:
            connection.execute("DELETE FROM trades_raw WHERE wallet = ?", (wallet,))
        connection.commit()


def _run_step(
    name: str,
    wallet: str,
    settings: Settings,
    *,
    skip_fetch: bool,
    version: int,
    page_limit: int | None = None,
    max_pages: int | None = None,
    fail_fast: bool = False,
    page_overlap: int | None = None,
    stabilization_sweeps: int = 3,
    target_unique_count: int | None = None,
) -> ValidationStep:
    pipeline = run_pipeline(
        wallet,
        settings=settings,
        skip_fetch=skip_fetch,
        skip_enrichment=True,
        version=version,
        page_limit=page_limit,
        max_pages=max_pages,
        fail_on_unmatched_sells=fail_fast,
        page_overlap=page_overlap,
        stabilization_sweeps=stabilization_sweeps,
        target_unique_count=target_unique_count,
    )
    snapshot = snapshot_wallet_state(wallet, settings, version=version)
    inventory_violations = validate_inventory_conservation(wallet, settings, version=version)
    if fail_fast:
        if snapshot.normalization_error_count or snapshot.quarantine_count:
            raise RuntimeError(
                f"{name} failed due to quarantined or malformed rows: "
                f"errors={snapshot.normalization_error_count} "
                f"quarantined={snapshot.quarantine_count}"
            )
        if inventory_violations:
            raise RuntimeError(
                f"{name} failed inventory conservation with {len(inventory_violations)} violations."
            )
    return ValidationStep(
        name=name,
        pipeline=pipeline,
        snapshot=snapshot,
        inventory_violations=inventory_violations,
    )


def _step_state_hash(step: ValidationStep) -> tuple[str, str, str]:
    return (
        step.snapshot.event_hash,
        step.snapshot.position_hash,
        step.snapshot.metrics_hash,
    )


def _pagination_db_path(base_path: Path, suffix: str) -> Path:
    return base_path.with_name(f"{base_path.stem}_{suffix}{base_path.suffix}")


def _pages_needed_for_target(target_unique_count: int, page_limit: int, page_overlap: int) -> int:
    if target_unique_count <= 0:
        return 1
    step = _page_step(page_limit, page_overlap)
    if target_unique_count <= page_limit:
        return 1
    remaining = target_unique_count - page_limit
    return 1 + math.ceil(remaining / step)


def run_live_validation(
    wallet: str,
    settings: Settings,
    *,
    version: int = 1,
    page_limit: int | None = None,
    max_pages: int | None = None,
    pagination_small_limit: int | None = None,
    pagination_large_limit: int | None = None,
    pagination_record_cap: int = 200,
    fail_fast: bool = False,
    page_overlap: int | None = None,
    stabilization_sweeps: int = 3,
) -> ValidationReport:
    initialize_database(settings)

    baseline = _run_step(
        "baseline",
        wallet,
        settings,
        skip_fetch=False,
        version=version,
        page_limit=page_limit,
        max_pages=max_pages,
        fail_fast=fail_fast,
        page_overlap=page_overlap,
        stabilization_sweeps=stabilization_sweeps,
    )
    rerun = _run_step(
        "rerun",
        wallet,
        settings,
        skip_fetch=False,
        version=version,
        page_limit=page_limit,
        max_pages=max_pages,
        fail_fast=fail_fast,
        page_overlap=page_overlap,
        stabilization_sweeps=stabilization_sweeps,
    )

    clear_wallet_state(wallet, settings, version=version, include_raw=False)
    rebuild = _run_step(
        "rebuild",
        wallet,
        settings,
        skip_fetch=True,
        version=version,
        fail_fast=fail_fast,
        page_overlap=page_overlap,
        stabilization_sweeps=stabilization_sweeps,
    )

    rerun_matches_baseline_without_new_data: bool | None
    rerun_new_raw = rerun.pipeline.ingestion.inserted_count if rerun.pipeline.ingestion else 0
    rerun_new_events = rerun.pipeline.normalization.inserted_count
    if rerun_new_raw == 0 and rerun_new_events == 0:
        rerun_matches_baseline_without_new_data = (
            _step_state_hash(rerun) == _step_state_hash(baseline)
        )
    else:
        rerun_matches_baseline_without_new_data = None

    pagination_small = None
    pagination_large = None
    pagination_matches = None

    if pagination_small_limit and pagination_large_limit:
        small_db = _pagination_db_path(settings.database_path, f"p{pagination_small_limit}")
        large_db = _pagination_db_path(settings.database_path, f"p{pagination_large_limit}")

        for db_path in (small_db, large_db):
            if db_path.exists():
                db_path.unlink()

        small_settings = get_settings(small_db)
        large_settings = get_settings(large_db)
        initialize_database(small_settings)
        initialize_database(large_settings)

        small_overlap = (
            _default_page_overlap(pagination_small_limit, 2)
            if page_overlap is None
            else page_overlap
        )
        large_overlap = (
            _default_page_overlap(pagination_large_limit, 2)
            if page_overlap is None
            else page_overlap
        )

        small_pages = _pages_needed_for_target(
            pagination_record_cap,
            pagination_small_limit,
            small_overlap,
        )
        large_pages = _pages_needed_for_target(
            pagination_record_cap,
            pagination_large_limit,
            large_overlap,
        )

        pagination_small = _run_step(
            "pagination_small",
            wallet,
            small_settings,
            skip_fetch=False,
            version=version,
            page_limit=pagination_small_limit,
            max_pages=small_pages,
            fail_fast=fail_fast,
            page_overlap=page_overlap,
            stabilization_sweeps=stabilization_sweeps,
            target_unique_count=pagination_record_cap,
        )
        pagination_large = _run_step(
            "pagination_large",
            wallet,
            large_settings,
            skip_fetch=False,
            version=version,
            page_limit=pagination_large_limit,
            max_pages=large_pages,
            fail_fast=fail_fast,
            page_overlap=page_overlap,
            stabilization_sweeps=stabilization_sweeps,
            target_unique_count=pagination_record_cap,
        )

        small_top_count, small_top_hash = hash_recent_trade_snapshot(
            wallet,
            small_settings,
            top_n=pagination_record_cap,
        )
        large_top_count, large_top_hash = hash_recent_trade_snapshot(
            wallet,
            large_settings,
            top_n=pagination_record_cap,
        )

        pagination_matches = (
            small_top_count == pagination_record_cap
            and large_top_count == pagination_record_cap
            and small_top_hash == large_top_hash
        )

    return ValidationReport(
        wallet=wallet,
        baseline=baseline,
        rerun=rerun,
        rebuild=rebuild,
        rebuild_matches_baseline=_step_state_hash(rebuild) == _step_state_hash(baseline),
        rerun_matches_baseline_without_new_data=rerun_matches_baseline_without_new_data,
        pagination_small=pagination_small,
        pagination_large=pagination_large,
        pagination_matches=pagination_matches,
    )


def _format_step(step: ValidationStep) -> list[str]:
    ingestion = step.pipeline.ingestion
    ingestion_summary = (
        f"fetched={ingestion.fetched_count} new_raw={ingestion.inserted_count} "
        f"duplicates={ingestion.duplicate_count} status={ingestion.status}"
        if ingestion
        else "fetched=0 new_raw=0 duplicates=0 status=SKIPPED"
    )
    violations = len(step.inventory_violations)

    return [
        f"[{step.name.upper()}]",
        f"  fetch: {ingestion_summary}",
        (
            "  normalize: "
            f"processed={step.pipeline.normalization.processed_count} "
            f"new_events={step.pipeline.normalization.inserted_count} "
            f"skipped={step.pipeline.normalization.skipped_count} "
            f"quarantined={step.pipeline.normalization.quarantined_count} "
            f"failed={step.pipeline.normalization.failed_count}"
        ),
        (
            "  recon: "
            f"events={step.pipeline.reconstruction.event_count} "
            f"closed={step.pipeline.reconstruction.closed_positions} "
            f"open={step.pipeline.reconstruction.open_positions} "
            f"unmatched_sells_volume={step.pipeline.reconstruction.unmatched_sells_volume:.6f}"
        ),
        (
            "  snapshot: "
            f"raw={step.snapshot.raw_count} "
            f"events={step.snapshot.event_count} "
            f"closed={step.snapshot.closed_positions} "
            f"open={step.snapshot.open_positions} "
            f"errors={step.snapshot.normalization_error_count} "
            f"quarantined={step.snapshot.quarantine_count}"
        ),
        f"  inventory_violations={violations}",
    ]


def print_validation_report(report: ValidationReport, *, verbose: bool = False) -> None:
    lines = [f"wallet={report.wallet}"]
    lines.extend(_format_step(report.baseline))
    lines.extend(_format_step(report.rerun))
    lines.extend(_format_step(report.rebuild))
    lines.append(f"rebuild_matches_baseline={report.rebuild_matches_baseline}")
    lines.append(
        "rerun_matches_baseline_without_new_data="
        f"{report.rerun_matches_baseline_without_new_data}"
    )

    if report.pagination_small and report.pagination_large:
        lines.extend(_format_step(report.pagination_small))
        lines.extend(_format_step(report.pagination_large))
        lines.append(f"pagination_matches={report.pagination_matches}")

    if verbose:
        for step in (
            report.baseline,
            report.rerun,
            report.rebuild,
            report.pagination_small,
            report.pagination_large,
        ):
            if step is None:
                continue
            lines.append(
                f"[{step.name.upper()}_HASHES] "
                f"events={step.snapshot.event_hash} "
                f"positions={step.snapshot.position_hash} "
                f"metrics={step.snapshot.metrics_hash}"
            )
            for violation in step.inventory_violations:
                lines.append(
                    f"[{step.name.upper()}_INVENTORY_VIOLATION] "
                    f"market_id={violation.market_id} "
                    f"outcome={violation.outcome} "
                    f"buys={violation.total_buys} "
                    f"sells={violation.total_sells} "
                    f"expected_remaining={violation.expected_remaining} "
                    f"actual_remaining={violation.actual_remaining}"
                )

    print("\n".join(lines))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run live-wallet validation for TIP V1.")
    parser.add_argument("--wallet", required=True, help="0x-prefixed Polymarket wallet.")
    parser.add_argument("--db-path", help="SQLite path used for the main validation run.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--page-limit", type=int, help="Main run fetch page size.")
    parser.add_argument("--max-pages", type=int, help="Main run fetch max pages.")
    parser.add_argument(
        "--pagination-small-limit",
        type=int,
        help="Optional small page size for the pagination variance check.",
    )
    parser.add_argument(
        "--pagination-large-limit",
        type=int,
        help="Optional large page size for the pagination variance check.",
    )
    parser.add_argument(
        "--pagination-record-cap",
        type=int,
        default=200,
        help="Approximate records to fetch per pagination comparison branch.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on quarantined rows, inventory violations, or negative inventory.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print snapshot hashes and detailed inventory violations.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    report = run_live_validation(
        args.wallet,
        settings=settings,
        version=args.version,
        page_limit=args.page_limit,
        max_pages=args.max_pages,
        pagination_small_limit=args.pagination_small_limit,
        pagination_large_limit=args.pagination_large_limit,
        pagination_record_cap=args.pagination_record_cap,
        fail_fast=args.fail_fast,
    )
    print_validation_report(report, verbose=args.verbose)


if __name__ == "__main__":
    main()
