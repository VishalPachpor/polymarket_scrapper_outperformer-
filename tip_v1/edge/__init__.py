from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tip_v1.edge.extract_regime_edges import extract_regime_edges as _extract_regime_edges_type

__all__ = ["extract_regime_edges"]


def extract_regime_edges(*args, **kwargs):
    from tip_v1.edge.extract_regime_edges import extract_regime_edges as _extract_regime_edges

    return _extract_regime_edges(*args, **kwargs)
