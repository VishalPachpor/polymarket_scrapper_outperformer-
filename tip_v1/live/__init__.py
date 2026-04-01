from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.live.trade_ingestor import (
        LiveIngestionConfig as _LiveIngestionConfigType,
        LiveIngestionCycleResult as _LiveIngestionCycleResultType,
    )

__all__ = [
    "LiveIngestionConfig",
    "LiveIngestionCycleResult",
    "ingest_recent_trade_stream",
    "run_live_ingestion",
]


def __getattr__(name: str):
    if name in __all__:
        from tip_v1.live import trade_ingestor as _trade_ingestor

        return getattr(_trade_ingestor, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
