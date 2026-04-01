from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from tip_v1.analytics.compute_metrics import MetricsResult, compute_metrics
from tip_v1.cli.report import print_report
from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database
from tip_v1.enrichment.compute_path_metrics import PathMetricsResult, compute_position_path_metrics
from tip_v1.enrichment.fetch_price_history import PriceHistoryFetchResult, fetch_wallet_price_history
from tip_v1.ingestion.fetch_trades import IngestionResult, ingest_trades
from tip_v1.normalization.normalize_trades import NormalizationResult, normalize_trades
from tip_v1.reconstruction.reconstruct_positions import (
    ReconstructionResult,
    reconstruct_positions,
)


@dataclass(frozen=True)
class PipelineResult:
    ingestion: IngestionResult | None
    normalization: NormalizationResult
    reconstruction: ReconstructionResult
    metrics: MetricsResult
    price_history: PriceHistoryFetchResult | None = None
    path_metrics: PathMetricsResult | None = None


def run_pipeline(
    wallet: str,
    settings: Settings | None = None,
    *,
    skip_fetch: bool = False,
    skip_enrichment: bool = False,
    version: int = 1,
    page_limit: int | None = None,
    max_pages: int | None = None,
    fail_on_unmatched_sells: bool = False,
    page_overlap: int | None = None,
    stabilization_sweeps: int = 5,
    stable_sweep_threshold: int = 2,
    target_unique_count: int | None = None,
) -> PipelineResult:
    settings = settings or get_settings()
    initialize_database(settings)

    ingestion_result = None
    if not skip_fetch:
        ingestion_result = ingest_trades(
            wallet,
            settings=settings,
            page_limit=page_limit,
            max_pages=max_pages,
            page_overlap=page_overlap,
            stabilization_sweeps=stabilization_sweeps,
            stable_sweep_threshold=stable_sweep_threshold,
            target_unique_count=target_unique_count,
        )

    normalization_result = normalize_trades(wallet=wallet, settings=settings)
    reconstruction_result = reconstruct_positions(
        wallet,
        settings=settings,
        version=version,
        fail_on_unmatched_sells=fail_on_unmatched_sells,
    )
    metrics_result = compute_metrics(wallet, settings=settings, version=version)

    price_history_result = None
    path_metrics_result = None
    if not skip_enrichment:
        price_history_result = fetch_wallet_price_history(wallet, settings=settings)
        path_metrics_result = compute_position_path_metrics(
            wallet, settings=settings, version=version,
        )
    else:
        print("[WARNING] Enrichment skipped — path metrics not computed.", file=sys.stderr)

    return PipelineResult(
        ingestion=ingestion_result,
        normalization=normalization_result,
        reconstruction=reconstruction_result,
        metrics=metrics_result,
        price_history=price_history_result,
        path_metrics=path_metrics_result,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TIP V1 pipeline for one wallet.")
    parser.add_argument("--wallet", required=True, help="0x-prefixed Polymarket wallet.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip API ingestion and use already stored raw trades.",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        help="Override trades fetched per page for this run.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Override the number of pages fetched for this run.",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Reconstruction logic version to compute.",
    )
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip price history fetch and path metrics computation.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)

    result = run_pipeline(
        args.wallet,
        settings=settings,
        skip_fetch=args.skip_fetch,
        skip_enrichment=args.skip_enrichment,
        version=args.version,
        page_limit=args.page_limit,
        max_pages=args.max_pages,
    )

    if result.ingestion:
        print(
            "[FETCH]"
            f" fetched={result.ingestion.fetched_count}"
            f" new_raw={result.ingestion.inserted_count}"
            f" duplicates={result.ingestion.duplicate_count}"
            f" run_id={result.ingestion.run_id}"
            f" status={result.ingestion.status}"
        )

    print(
        "[NORMALIZE]"
        f" processed={result.normalization.processed_count}"
        f" new_events={result.normalization.inserted_count}"
        f" skipped={result.normalization.skipped_count}"
        f" quarantined={result.normalization.quarantined_count}"
        f" failed={result.normalization.failed_count}"
    )
    print(
        "[RECON]"
        f" events={result.reconstruction.event_count}"
        f" closed={result.reconstruction.closed_positions}"
        f" open={result.reconstruction.open_positions}"
        f" unmatched_sells_volume={result.reconstruction.unmatched_sells_volume:.6f}"
    )

    if result.price_history:
        print(
            "[ENRICH]"
            f" assets_requested={result.price_history.assets_requested}"
            f" assets_fetched={result.price_history.assets_fetched}"
            f" assets_failed={result.price_history.assets_failed}"
            f" samples_fetched={result.price_history.fetched_samples}"
            f" samples_inserted={result.price_history.inserted_samples}"
        )
    if result.path_metrics:
        print(
            "[PATH]"
            f" positions_processed={result.path_metrics.positions_processed}"
            f" with_path_data={result.path_metrics.positions_with_path_data}"
        )

    print()
    print_report(args.wallet, settings=settings, version=args.version)


if __name__ == "__main__":
    main()
