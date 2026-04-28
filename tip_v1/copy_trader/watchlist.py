from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WatchedWallet:
    address: str
    pseudonym: str
    weight: float          # 0.0 - 1.0, share of capital allocation
    size_pct: float        # fraction of source-wallet trade size to mirror (0.10 = 10%)
    max_per_trade_usdc: float
    notes: str = ""


# Top IPL piggyback targets, ranked by April 2026 form.
# Rectangular-Irony is the standout: 23W/4L across 27 matches, +$195K lifetime
# (100% IPL-only — edge does not generalize outside cricket).
_DEFAULT_WATCHLIST: tuple[WatchedWallet, ...] = (
    WatchedWallet(
        address="0x82ff01408b945af138d3c4619dcf876387d52b09",
        pseudonym="Rectangular-Irony",
        weight=0.50,
        size_pct=0.10,
        max_per_trade_usdc=200.0,
        notes="Pure IPL specialist; 85% match win rate; primary signal source",
    ),
    WatchedWallet(
        address="0x69adf26878af1b1ee83e3144787f37bc8c4b21db",
        pseudonym="Zigzag-Logistics",
        weight=0.20,
        size_pct=0.10,
        max_per_trade_usdc=150.0,
        notes="64% win rate, active scalper, good signal frequency",
    ),
    WatchedWallet(
        address="0x507e52ef684ca2dd91f90a9d26d149dd3288beae",
        pseudonym="Parallel-Flock",
        weight=0.15,
        size_pct=0.05,
        max_per_trade_usdc=150.0,
        notes="Directional buyer (no scalping). Use as conviction signal",
    ),
    WatchedWallet(
        address="0xa4b7b1814b0da33f2b61be4939976898aa476008",
        pseudonym="Innocent-Classmate",
        weight=0.15,
        size_pct=0.05,
        max_per_trade_usdc=150.0,
        notes="Directional buyer; 64% match win rate",
    ),
)


def _parse_env_watchlist(raw: str) -> tuple[WatchedWallet, ...] | None:
    """Optional override via env: COPY_TRADER_WATCHLIST="addr:pseudo:weight:size_pct:max_usdc,..."."""
    raw = raw.strip()
    if not raw:
        return None
    out: list[WatchedWallet] = []
    for entry in raw.split(","):
        parts = entry.strip().split(":")
        if len(parts) < 2:
            continue
        addr = parts[0].strip().lower()
        pseudo = parts[1].strip()
        weight = float(parts[2]) if len(parts) > 2 else 0.25
        size_pct = float(parts[3]) if len(parts) > 3 else 0.10
        max_usdc = float(parts[4]) if len(parts) > 4 else 100.0
        out.append(WatchedWallet(addr, pseudo, weight, size_pct, max_usdc))
    return tuple(out) if out else None


def get_watchlist() -> dict[str, WatchedWallet]:
    """Return wallet-address-keyed watchlist (env override > defaults)."""
    raw = os.getenv("COPY_TRADER_WATCHLIST", "")
    parsed = _parse_env_watchlist(raw)
    items = parsed if parsed is not None else _DEFAULT_WATCHLIST
    return {w.address.lower(): w for w in items}
