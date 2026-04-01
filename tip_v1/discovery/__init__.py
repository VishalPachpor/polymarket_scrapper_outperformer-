from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.discovery.orchestrator import DiscoveryCycleResult
    from tip_v1.config import Settings

__all__ = ["run_discovery_cycle"]


def run_discovery_cycle(*args, **kwargs):
    from tip_v1.discovery.orchestrator import run_discovery_cycle as _run_discovery_cycle

    return _run_discovery_cycle(*args, **kwargs)
