from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.simulator.simulate_strategy import simulate as _simulate_type

__all__ = ["simulate"]


def simulate(*args, **kwargs):
    from tip_v1.simulator.simulate_strategy import simulate as _simulate

    return _simulate(*args, **kwargs)
