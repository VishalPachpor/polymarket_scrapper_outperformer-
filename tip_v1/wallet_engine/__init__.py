from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.wallet_engine.signal_builder import (
        WalletSignal as _WalletSignalType,
    )
    from tip_v1.wallet_engine.trade_stream import (
        DbBackedTradeStream as _DbBackedTradeStreamType,
        Trade as _TradeType,
        TradeStreamConfig as _TradeStreamConfigType,
    )
    from tip_v1.wallet_engine.wallet_paper_tracker import (
        WalletPaperMetrics as _WalletPaperMetricsType,
        WalletPaperTracker as _WalletPaperTrackerType,
        WalletPaperTrackerConfig as _WalletPaperTrackerConfigType,
        WalletPaperTrackerCycle as _WalletPaperTrackerCycleType,
    )
    from tip_v1.wallet_engine.wallet_runtime import (
        WalletEngineRuntime as _WalletEngineRuntimeType,
        WalletRuntimeConfig as _WalletRuntimeConfigType,
        WalletRuntimeCycle as _WalletRuntimeCycleType,
    )

__all__ = [
    "DbBackedTradeStream",
    "Trade",
    "TradeStreamConfig",
    "WalletEngineRuntime",
    "WalletPaperMetrics",
    "WalletPaperTracker",
    "WalletPaperTrackerConfig",
    "WalletPaperTrackerCycle",
    "WalletRuntimeConfig",
    "WalletRuntimeCycle",
    "WalletSignal",
]


def __getattr__(name: str):
    if name in {"DbBackedTradeStream", "Trade", "TradeStreamConfig"}:
        from tip_v1.wallet_engine import trade_stream as _trade_stream

        return getattr(_trade_stream, name)
    if name in {"WalletSignal"}:
        from tip_v1.wallet_engine import signal_builder as _signal_builder

        return getattr(_signal_builder, name)
    if name in {
        "WalletPaperMetrics",
        "WalletPaperTracker",
        "WalletPaperTrackerConfig",
        "WalletPaperTrackerCycle",
    }:
        from tip_v1.wallet_engine import wallet_paper_tracker as _wallet_paper_tracker

        return getattr(_wallet_paper_tracker, name)
    if name in {"WalletEngineRuntime", "WalletRuntimeConfig", "WalletRuntimeCycle"}:
        from tip_v1.wallet_engine import wallet_runtime as _wallet_runtime

        return getattr(_wallet_runtime, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
