from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.signals.momentum_trigger import (
        MomentumSignal as _MomentumSignalType,
        MomentumTriggerConfig as _MomentumTriggerConfigType,
        PriceSample as _PriceSampleType,
    )

__all__ = [
    "MomentumSignal",
    "MomentumTriggerConfig",
    "PriceSample",
    "evaluate_momentum_signal",
    "generate_momentum_signal_from_db",
    "load_recent_trade_samples",
]


def __getattr__(name: str):
    if name in __all__:
        from tip_v1.signals import momentum_trigger as _momentum_trigger

        return getattr(_momentum_trigger, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
