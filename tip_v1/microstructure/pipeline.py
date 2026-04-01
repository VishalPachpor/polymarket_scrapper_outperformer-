from __future__ import annotations

from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database
from tip_v1.microstructure.compute_features import (
    MicrostructureFeatureResult,
    compute_microstructure_features,
)
from tip_v1.microstructure.fetch_microstructure import (
    MicrostructureIngestionResult,
    ingest_orderbooks_for_wallet_assets,
)
from tip_v1.microstructure.join_with_trades import (
    TradeContextJoinResult,
    join_microstructure_with_trades,
)
from tip_v1.microstructure.normalize_microstructure import (
    MicrostructureNormalizationResult,
    normalize_book_snapshots,
)


@dataclass(frozen=True)
class MicrostructurePipelineResult:
    ingestion: MicrostructureIngestionResult
    normalization: MicrostructureNormalizationResult
    features: MicrostructureFeatureResult
    join: TradeContextJoinResult


def run_microstructure_pipeline(
    wallet: str,
    settings: Settings | None = None,
) -> MicrostructurePipelineResult:
    settings = settings or get_settings()
    initialize_database(settings)

    ingestion = ingest_orderbooks_for_wallet_assets(wallet, settings)
    normalization = normalize_book_snapshots(settings)
    features = compute_microstructure_features(settings)
    join = join_microstructure_with_trades(wallet, settings)

    return MicrostructurePipelineResult(
        ingestion=ingestion,
        normalization=normalization,
        features=features,
        join=join,
    )
