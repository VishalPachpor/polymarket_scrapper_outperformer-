from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.paper.paper_trade_runner import (
        PaperSignalMetrics as _PaperSignalMetricsType,
        PaperTradeConfig as _PaperTradeConfigType,
        PaperTradeRunner as _PaperTradeRunnerType,
    )

__all__ = [
    "PaperSignalMetrics",
    "PaperTradeConfig",
    "PaperTradeRunner",
    "compute_paper_signal_metrics",
]


def __getattr__(name: str):
    if name in __all__:
        from tip_v1.paper import paper_trade_runner as _paper_trade_runner

        return getattr(_paper_trade_runner, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
