from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import asdict, dataclass, field
from typing import Any

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection


# ---------------------------------------------------------------------------
# Feature dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WalletFeatures:
    total_trades: int
    buy_count: int
    sell_count: int
    closed_positions: int
    open_positions: int
    unique_markets: int

    sell_to_buy_ratio: float
    open_inventory_ratio: float
    two_sided_market_ratio: float

    avg_hold_time: float
    median_hold_time: float

    win_rate: float
    avg_pnl: float
    profit_factor: float

    mfe_capture_ratio: float
    avg_time_to_mfe: float
    avg_time_after_mfe: float

    extreme_entry_ratio: float
    mid_entry_ratio: float
    realized_pnl_per_minute: float
    total_realized_pnl: float
    active_days: float
    pnl_per_day: float
    pnl_per_trade: float
    capital_proxy: float
    return_proxy: float


# ---------------------------------------------------------------------------
# Classification types
# ---------------------------------------------------------------------------

TRADER_TYPES = (
    "STRUCTURED_INVENTORY",
    "REPRICING",
    "OUTCOME",
    "MOMENTUM",
    "NOISE",
)


@dataclass(frozen=True)
class TraderProfile:
    wallet: str
    trader_type: str
    confidence: float
    followability_score: float
    is_followable: bool
    features: WalletFeatures
    metrics: dict[str, Any]
    behavior: dict[str, Any]
    edge: tuple[str, ...]
    failures: tuple[str, ...]


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

def compute_features(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
) -> WalletFeatures:
    settings = settings or get_settings()
    min_pnl_sample = 20

    with managed_connection(settings) as connection:
        # --- trade counts ---
        counts = connection.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN side = 'BUY'  THEN 1 ELSE 0 END) AS buys,
                SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sells,
                COUNT(DISTINCT market_id) AS unique_markets
            FROM trade_events
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchone()

        total_trades = int(counts["total"] or 0)
        buy_count = int(counts["buys"] or 0)
        sell_count = int(counts["sells"] or 0)
        unique_markets = int(counts["unique_markets"] or 0)

        # --- position counts ---
        pos_counts = connection.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed,
                SUM(CASE WHEN status = 'OPEN'   THEN 1 ELSE 0 END) AS open_pos
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ?
            """,
            (wallet, version),
        ).fetchone()

        closed_positions = int(pos_counts["closed"] or 0)
        open_positions = int(pos_counts["open_pos"] or 0)
        total_positions = closed_positions + open_positions

        # --- two-sided market ratio ---
        two_sided_row = connection.execute(
            """
            SELECT COUNT(*) AS two_sided
            FROM (
                SELECT market_id
                FROM trade_events
                WHERE wallet = ?
                GROUP BY market_id
                HAVING SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) > 0
                   AND SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) > 0
            )
            """,
            (wallet,),
        ).fetchone()
        two_sided_count = int(two_sided_row["two_sided"] or 0)
        two_sided_market_ratio = (
            two_sided_count / unique_markets if unique_markets > 0 else 0.0
        )

        # --- hold times (closed positions) ---
        hold_times_rows = connection.execute(
            """
            SELECT duration
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ? AND status = 'CLOSED' AND duration IS NOT NULL
            ORDER BY duration
            """,
            (wallet, version),
        ).fetchall()
        hold_times = [int(row["duration"]) for row in hold_times_rows]
        avg_hold_time = statistics.mean(hold_times) if hold_times else 0.0
        median_hold_time = statistics.median(hold_times) if hold_times else 0.0

        # --- win/loss/profit factor ---
        pnl_rows = connection.execute(
            """
            SELECT pnl
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ? AND status = 'CLOSED'
            """,
            (wallet, version),
        ).fetchall()
        pnls = [float(row["pnl"]) for row in pnl_rows if row["pnl"] is not None]

        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        decisive = wins + losses
        win_rate = (wins / decisive) if decisive > 0 else 0.0
        avg_pnl = statistics.mean(pnls) if pnls else 0.0

        gross_profit = sum(p for p in pnls if p > 0)
        gross_loss = abs(sum(p for p in pnls if p < 0))
        _PROFIT_FACTOR_CAP = 3.0
        _MIN_PROFIT_FACTOR_SAMPLE = 10
        if gross_loss > 0:
            profit_factor = min(gross_profit / gross_loss, _PROFIT_FACTOR_CAP)
        elif gross_profit > 0 and wins >= _MIN_PROFIT_FACTOR_SAMPLE:
            profit_factor = _PROFIT_FACTOR_CAP
        else:
            profit_factor = 0.0

        # --- MFE capture ratio + timing ---
        path_rows = connection.execute(
            """
            SELECT
                pm.mfe,
                pm.time_to_mfe,
                p.pnl,
                p.duration
            FROM position_path_metrics pm
            JOIN positions_reconstructed p ON p.id = pm.position_id
            WHERE pm.wallet = ? AND pm.version = ?
              AND p.status = 'CLOSED' AND p.pnl IS NOT NULL AND pm.mfe IS NOT NULL
            """,
            (wallet, version),
        ).fetchall()

        mfe_capture_ratios = []
        times_to_mfe = []
        times_after_mfe = []
        for row in path_rows:
            mfe = float(row["mfe"])
            pnl_val = float(row["pnl"])
            t_mfe = int(row["time_to_mfe"] or 0)
            duration = int(row["duration"] or 0)
            if mfe > 0:
                mfe_capture_ratios.append(pnl_val / mfe)
            times_to_mfe.append(t_mfe)
            times_after_mfe.append(max(0, duration - t_mfe))

        mfe_capture_ratio = (
            statistics.mean(mfe_capture_ratios) if mfe_capture_ratios else 0.0
        )
        avg_time_to_mfe = statistics.mean(times_to_mfe) if times_to_mfe else 0.0
        avg_time_after_mfe = statistics.mean(times_after_mfe) if times_after_mfe else 0.0

        # --- entry price distribution ---
        entry_prices = connection.execute(
            """
            SELECT entry_price
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ? AND entry_price IS NOT NULL
            """,
            (wallet, version),
        ).fetchall()
        prices = [float(row["entry_price"]) for row in entry_prices]
        extreme_count = sum(1 for p in prices if p < 0.10 or p > 0.70)
        mid_count = sum(1 for p in prices if 0.30 <= p <= 0.70)
        extreme_entry_ratio = (extreme_count / len(prices)) if prices else 0.0
        mid_entry_ratio = (mid_count / len(prices)) if prices else 0.0

        # --- realized pnl per minute ---
        pnl_summary = connection.execute(
            """
            SELECT
                SUM(pnl) AS total_pnl,
                COUNT(*) AS closed_count,
                SUM(duration) / 60.0 AS total_minutes,
                MIN(entry_time) AS first_trade_ts,
                MAX(exit_time) AS last_trade_ts,
                SUM(entry_price * size) AS total_notional
            FROM positions_reconstructed
            WHERE wallet = ? AND version = ? AND status = 'CLOSED'
              AND pnl IS NOT NULL
            """,
            (wallet, version),
        ).fetchone()

        raw_total_pnl = float(pnl_summary["total_pnl"] or 0.0)
        total_minutes = float(pnl_summary["total_minutes"] or 0.0)
        first_trade_ts = pnl_summary["first_trade_ts"]
        last_trade_ts = pnl_summary["last_trade_ts"]
        total_notional = float(pnl_summary["total_notional"] or 0.0)

        if (
            first_trade_ts is not None
            and last_trade_ts is not None
            and int(last_trade_ts) > int(first_trade_ts)
        ):
            active_days = (int(last_trade_ts) - int(first_trade_ts)) / 86_400.0
        else:
            active_days = 0.0

        if closed_positions >= min_pnl_sample:
            realized_pnl_per_minute = (
                raw_total_pnl / total_minutes if total_minutes > 0 else 0.0
            )
            total_realized_pnl = raw_total_pnl
            pnl_per_day = raw_total_pnl / active_days if active_days > 0 else 0.0
            return_proxy = raw_total_pnl / total_notional if total_notional > 0 else 0.0
        else:
            realized_pnl_per_minute = 0.0
            total_realized_pnl = 0.0
            pnl_per_day = 0.0
            return_proxy = 0.0

        pnl_per_trade = raw_total_pnl / closed_positions if closed_positions > 0 else 0.0
        capital_proxy = total_notional / closed_positions if closed_positions > 0 else 0.0

    # --- ratios ---
    sell_to_buy_ratio = (sell_count / buy_count) if buy_count > 0 else 0.0
    open_inventory_ratio = (
        open_positions / total_positions if total_positions > 0 else 0.0
    )

    return WalletFeatures(
        total_trades=total_trades,
        buy_count=buy_count,
        sell_count=sell_count,
        closed_positions=closed_positions,
        open_positions=open_positions,
        unique_markets=unique_markets,
        sell_to_buy_ratio=sell_to_buy_ratio,
        open_inventory_ratio=open_inventory_ratio,
        two_sided_market_ratio=two_sided_market_ratio,
        avg_hold_time=avg_hold_time,
        median_hold_time=median_hold_time,
        win_rate=win_rate,
        avg_pnl=avg_pnl,
        profit_factor=profit_factor,
        mfe_capture_ratio=mfe_capture_ratio,
        avg_time_to_mfe=avg_time_to_mfe,
        avg_time_after_mfe=avg_time_after_mfe,
        extreme_entry_ratio=extreme_entry_ratio,
        mid_entry_ratio=mid_entry_ratio,
        realized_pnl_per_minute=realized_pnl_per_minute,
        total_realized_pnl=total_realized_pnl,
        active_days=active_days,
        pnl_per_day=pnl_per_day,
        pnl_per_trade=pnl_per_trade,
        capital_proxy=capital_proxy,
        return_proxy=return_proxy,
    )


# ---------------------------------------------------------------------------
# Classification (priority-ordered)
# ---------------------------------------------------------------------------

def classify_trader(f: WalletFeatures) -> str:
    # 1. Structured / Inventory Builder
    if (
        f.open_inventory_ratio > 0.80
        and f.sell_to_buy_ratio < 0.20
        and f.two_sided_market_ratio > 0.40
        and f.closed_positions < 100
    ):
        return "STRUCTURED_INVENTORY"

    # 2. Repricing / Inventory Trader
    if (
        f.two_sided_market_ratio > 0.50
        and 0.70 <= f.sell_to_buy_ratio <= 1.50
        and f.median_hold_time < 7200  # 2 hours
        and f.mfe_capture_ratio > 0.60
    ):
        return "REPRICING"

    # 3. Outcome / Resolution Trader
    if (
        f.median_hold_time > 21600  # 6 hours
        and f.sell_to_buy_ratio < 0.30
        and f.closed_positions > 50
    ):
        return "OUTCOME"

    # 4. Momentum Trader
    if (
        f.two_sided_market_ratio < 0.30
        and f.extreme_entry_ratio < 0.30
        and 300 <= f.median_hold_time <= 7200  # 5 min to 2 hours
        and f.mfe_capture_ratio < 0.50
    ):
        return "MOMENTUM"

    # 5. Noise (fallback)
    return "NOISE"


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def compute_confidence(f: WalletFeatures) -> float:
    total_positions = f.closed_positions + f.open_positions
    if total_positions == 0 or f.total_trades == 0:
        return 0.0

    trade_volume_score = min(1.0, math.log10(max(1, f.total_trades)) / 3.0)
    closure_ratio = f.closed_positions / total_positions
    consistency = abs(f.win_rate - 0.5) * 2 if f.closed_positions > 0 else 0.0

    return round(min(1.0, trade_volume_score * closure_ratio * consistency), 4)


def compute_followability(f: WalletFeatures) -> float:
    total_positions = f.closed_positions + f.open_positions
    if total_positions == 0 or f.total_trades == 0:
        return 0.0

    closed_positions_ratio = f.closed_positions / total_positions
    sell_component = max(0.0, min(1.0, f.sell_to_buy_ratio))
    mfe_component = max(0.0, min(1.0, f.mfe_capture_ratio))
    sample_component = max(0.0, min(1.0, f.closed_positions / 50.0))
    win_component = (
        max(0.0, min(1.0, f.win_rate / 0.55))
        if f.closed_positions > 0
        else 0.0
    )
    consistency = (sample_component + win_component) / 2.0

    return round(
        min(1.0, closed_positions_ratio * sell_component * mfe_component * consistency),
        4,
    )


def is_followable_wallet(f: WalletFeatures) -> bool:
    return (
        f.closed_positions > 50
        and f.sell_to_buy_ratio > 0.30
        and f.mfe_capture_ratio > 0.50
    )


# ---------------------------------------------------------------------------
# Behavior + Edge + Failure extraction
# ---------------------------------------------------------------------------

def extract_behavior(f: WalletFeatures) -> dict[str, Any]:
    if f.extreme_entry_ratio > 0.50:
        entry_style = "extreme"
    elif f.mid_entry_ratio > 0.50:
        entry_style = "mid-range"
    else:
        entry_style = "mixed"

    return {
        "entry_style": entry_style,
        "two_sided": f.two_sided_market_ratio > 0.40,
        "scales_positions": f.buy_count > f.unique_markets * 2 if f.unique_markets > 0 else False,
        "active_exit": f.sell_to_buy_ratio > 0.50,
    }


def extract_edge(f: WalletFeatures) -> list[str]:
    edge: list[str] = []
    if f.extreme_entry_ratio > 0.50:
        edge.append("Enters at extreme probabilities (<0.10 or >0.70)")
    if f.mfe_capture_ratio > 0.70:
        edge.append("Strong exit timing — captures most of favorable excursion")
    if f.profit_factor > 2.0 and f.closed_positions > 20:
        edge.append("High profit factor with meaningful sample size")
    if f.two_sided_market_ratio > 0.60 and f.sell_to_buy_ratio > 0.70:
        edge.append("Active two-sided trading across markets")
    if f.win_rate > 0.60 and f.closed_positions > 30:
        edge.append("Consistent winner with sufficient sample")
    return edge


def extract_failures(f: WalletFeatures) -> list[str]:
    failures: list[str] = []
    if f.avg_time_after_mfe > f.avg_time_to_mfe * 2 and f.avg_time_to_mfe > 0:
        failures.append("Holds too long after peak — gives back gains")
    if f.mid_entry_ratio > 0.50:
        failures.append("Enters at mid-range probabilities — low edge zone")
    if f.mfe_capture_ratio < 0.30 and f.closed_positions > 10:
        failures.append("Poor exit timing — captures little of favorable move")
    if f.win_rate < 0.45 and f.profit_factor < 1.0 and f.closed_positions > 20:
        failures.append("Below breakeven with meaningful sample")
    if f.open_inventory_ratio > 0.90 and f.closed_positions < 5:
        failures.append("Almost no exits — exposure is unmanaged")
    return failures


# ---------------------------------------------------------------------------
# Top-level profiler
# ---------------------------------------------------------------------------

def _json_dump(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _persist_profile(
    profile: TraderProfile,
    settings: Settings,
    *,
    version: int,
) -> None:
    features_json = _json_dump(asdict(profile.features))
    metrics_json = _json_dump(profile.metrics)
    behavior_json = _json_dump(profile.behavior)
    edge_json = _json_dump(profile.edge)
    failures_json = _json_dump(profile.failures)

    with managed_connection(settings) as connection:
        connection.execute(
            """
            INSERT INTO wallet_profiles (
                wallet, version, trader_type, confidence,
                features_json, metrics_json, behavior_json,
                edge_json, failures_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(wallet) DO UPDATE SET
                version = excluded.version,
                trader_type = excluded.trader_type,
                confidence = excluded.confidence,
                features_json = excluded.features_json,
                metrics_json = excluded.metrics_json,
                behavior_json = excluded.behavior_json,
                edge_json = excluded.edge_json,
                failures_json = excluded.failures_json,
                updated_at = excluded.updated_at
            """,
            (
                profile.wallet, version, profile.trader_type, profile.confidence,
                features_json, metrics_json, behavior_json,
                edge_json, failures_json,
            ),
        )
        connection.execute(
            """
            INSERT INTO wallet_profile_runs (
                wallet, version, trader_type, confidence,
                features_json, metrics_json, behavior_json,
                edge_json, failures_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile.wallet, version, profile.trader_type, profile.confidence,
                features_json, metrics_json, behavior_json,
                edge_json, failures_json,
            ),
        )
        connection.commit()


def load_stored_profile(
    wallet: str,
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    settings = settings or get_settings()
    with managed_connection(settings) as connection:
        row = connection.execute(
            """
            SELECT wallet, version, trader_type, confidence,
                   features_json, metrics_json, behavior_json,
                   edge_json, failures_json, updated_at
            FROM wallet_profiles
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchone()

    if row is None:
        return None

    metrics = json.loads(row["metrics_json"])

    return {
        "wallet": row["wallet"],
        "version": row["version"],
        "trader_type": row["trader_type"],
        "confidence": float(row["confidence"]),
        "followability_score": float(metrics.get("followability_score", 0.0)),
        "is_followable": bool(metrics.get("is_followable", False)),
        "features": json.loads(row["features_json"]),
        "metrics": metrics,
        "behavior": json.loads(row["behavior_json"]),
        "edge": json.loads(row["edge_json"]),
        "failures": json.loads(row["failures_json"]),
        "updated_at": row["updated_at"],
    }


def profile_wallet(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
    persist: bool = True,
) -> TraderProfile:
    settings = settings or get_settings()
    initialize_database(settings)

    features = compute_features(wallet, settings=settings, version=version)
    trader_type = classify_trader(features)
    confidence = compute_confidence(features)
    followability_score = compute_followability(features)
    is_followable = is_followable_wallet(features)
    behavior = extract_behavior(features)
    edge = extract_edge(features)
    failures = extract_failures(features)

    _PNL_PER_MIN_NORM_CAP = 0.1
    pnl_per_minute_score = min(
        1.0,
        max(0.0, features.realized_pnl_per_minute / _PNL_PER_MIN_NORM_CAP),
    )

    metrics = {
        "win_rate": features.win_rate,
        "avg_pnl": features.avg_pnl,
        "profit_factor": features.profit_factor,
        "avg_hold_time": features.avg_hold_time,
        "median_hold_time": features.median_hold_time,
        "closed_positions": features.closed_positions,
        "open_positions": features.open_positions,
        "recycling_ratio": features.sell_to_buy_ratio,
        "pnl_per_minute": features.realized_pnl_per_minute,
        "total_pnl": features.total_realized_pnl,
        "pnl_per_day": features.pnl_per_day,
        "pnl_per_trade": features.pnl_per_trade,
        "capital_proxy": features.capital_proxy,
        "return_proxy": features.return_proxy,
        "pnl_per_minute_score": pnl_per_minute_score,
        "lifecycle_score": (
            features.closed_positions / (features.closed_positions + features.open_positions)
            if (features.closed_positions + features.open_positions) > 0
            else 0.0
        ),
        "followability_score": followability_score,
        "is_followable": is_followable,
    }

    profile = TraderProfile(
        wallet=wallet,
        trader_type=trader_type,
        confidence=confidence,
        followability_score=followability_score,
        is_followable=is_followable,
        features=features,
        metrics=metrics,
        behavior=behavior,
        edge=tuple(edge),
        failures=tuple(failures),
    )

    if persist:
        _persist_profile(profile, settings, version=version)

    return profile


# ---------------------------------------------------------------------------
# CLI output
# ---------------------------------------------------------------------------

_TYPE_LABELS = {
    "STRUCTURED_INVENTORY": "Structured / Inventory Builder",
    "REPRICING": "Repricing / Inventory Trader",
    "OUTCOME": "Outcome / Resolution Trader",
    "MOMENTUM": "Momentum Trader",
    "NOISE": "Noise / Unclassified",
}


def format_profile(profile: TraderProfile) -> str:
    f = profile.features
    lines = [
        f"Wallet: {profile.wallet}",
        f"Type: {_TYPE_LABELS.get(profile.trader_type, profile.trader_type)}",
        f"Confidence: {profile.confidence:.2%}",
        f"Followability: {profile.followability_score:.2%} ({'YES' if profile.is_followable else 'NO'})",
        "",
        "--- Metrics ---",
        f"Win Rate: {profile.metrics['win_rate']:.2%}",
        f"Avg PnL: {profile.metrics['avg_pnl']:.6f}",
        f"Profit Factor: {profile.metrics['profit_factor']:.2f}",
        f"PnL/min: {profile.metrics['pnl_per_minute']:.6f}  (score: {profile.metrics['pnl_per_minute_score']:.2f})",
        f"Total PnL: ${profile.metrics['total_pnl']:.2f}",
        f"PnL/day: ${profile.metrics['pnl_per_day']:.2f}",
        f"PnL/trade: ${profile.metrics['pnl_per_trade']:.2f}",
        f"Return proxy: {profile.metrics['return_proxy']:.4f}",
        f"Capital proxy: {profile.metrics['capital_proxy']:.2f}",
        f"Closed: {f.closed_positions}  Open: {f.open_positions}  Markets: {f.unique_markets}",
        f"Avg Hold: {f.avg_hold_time:.0f}s  Median Hold: {f.median_hold_time:.0f}s",
        "",
        "--- Behavior ---",
        f"Entry Style: {profile.behavior['entry_style']}",
        f"Two-Sided: {profile.behavior['two_sided']}",
        f"Scales Positions: {profile.behavior['scales_positions']}",
        f"Active Exit: {profile.behavior['active_exit']}",
        "",
        "--- Features ---",
        f"Trades: {f.total_trades} (buy={f.buy_count}, sell={f.sell_count})",
        f"Sell/Buy Ratio: {f.sell_to_buy_ratio:.2f}",
        f"Open Inventory Ratio: {f.open_inventory_ratio:.2%}",
        f"Two-Sided Market Ratio: {f.two_sided_market_ratio:.2%}",
        f"MFE Capture Ratio: {f.mfe_capture_ratio:.2f}",
        f"Avg Time to MFE: {f.avg_time_to_mfe:.0f}s",
        f"Avg Time After MFE: {f.avg_time_after_mfe:.0f}s",
        f"Extreme Entry Ratio: {f.extreme_entry_ratio:.2%}",
        f"Mid Entry Ratio: {f.mid_entry_ratio:.2%}",
    ]

    if profile.edge:
        lines.append("")
        lines.append("--- Edge ---")
        for item in profile.edge:
            lines.append(f"+ {item}")

    if profile.failures:
        lines.append("")
        lines.append("--- Failure Modes ---")
        for item in profile.failures:
            lines.append(f"- {item}")

    return "\n".join(lines)


def print_profile(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
) -> None:
    profile = profile_wallet(wallet, settings=settings, version=version)
    print(format_profile(profile))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile a Polymarket wallet.")
    parser.add_argument("--wallet", required=True, help="0x-prefixed Polymarket wallet.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    print_profile(args.wallet, settings=settings, version=args.version)


if __name__ == "__main__":
    main()
