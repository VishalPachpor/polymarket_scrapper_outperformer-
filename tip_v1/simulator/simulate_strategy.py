from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    min_entry_price: float = 0.30
    max_entry_price: float = 0.50
    include_sub_60s: bool = False
    min_signal_seconds: int = 60
    strong_signal_seconds: int = 180
    late_signal_seconds: int = 300
    strong_capture_ratio: float = 0.6
    late_capture_ratio: float = 0.4
    weak_capture_ratio: float = 0.25
    moderate_hold_penalty_seconds: int = 120
    heavy_hold_penalty_seconds: int = 300
    moderate_penalty_ratio: float = 0.2
    heavy_penalty_ratio: float = 0.5
    soft_exit_ratio: float = 0.8
    aggressive_early_penalty_ratio: float = 0.5
    stop_loss_fraction: float = 0.3
    execution_success_rate: float = 0.7
    slippage_per_trade: float = 0.01
    momentum_lookback_seconds: int = 60
    min_abs_price_change: float = 0.03
    max_abs_price_change: float = 0.08
    min_recent_trade_count: int = 8
    max_spread: float = 0.08
    min_imbalance_edge: float = 0.10
    min_volatility_30s: float = 0.01


@dataclass(frozen=True)
class SimulatedTrade:
    position_id: int
    wallet: str
    market_id: str
    outcome: str
    entry_price: float
    size: float
    entry_time: int
    duration: int
    effective_duration: int
    actual_pnl: float
    simulated_pnl: float
    pnl_multiplier: float
    mfe: float | None
    mae: float | None
    time_to_mfe: int | None
    time_after_mfe: int | None
    trigger_passed: bool


@dataclass(frozen=True)
class StrategySimulationResult:
    variant: str
    wallet: str | None
    regime: str
    total_trades: int
    trades_used: int
    win_rate: float
    avg_pnl: float
    total_pnl: float
    pnl_per_trade: float
    pnl_per_minute: float
    max_drawdown: float
    pnl_series: tuple[tuple[int, float, float], ...]
    trades: tuple[SimulatedTrade, ...]


VARIANT_CONFIGS: dict[str, StrategyConfig] = {
    "baseline": StrategyConfig(name="baseline"),
    "strict_timing": StrategyConfig(
        name="strict_timing",
    ),
    "soft_exit": StrategyConfig(
        name="soft_exit",
    ),
    "aggressive": StrategyConfig(
        name="aggressive",
        include_sub_60s=True,
    ),
}


def get_strategy_config(variant: str) -> StrategyConfig:
    try:
        return VARIANT_CONFIGS[variant]
    except KeyError as exc:
        raise ValueError(f"Unknown strategy variant: {variant}") from exc


def load_positions(
    connection,
    *,
    wallet: str | None = None,
    regime: str = "global",
    min_trades: int = 0,
    start_ts: int | None = None,
    end_ts: int | None = None,
    version: int = 1,
) -> list[dict]:
    clauses = ["p.status = 'CLOSED'", "p.version = ?"]
    params: list[object] = [version]

    if wallet:
        clauses.append("p.wallet = ?")
        params.append(wallet)
    elif regime != "global":
        clauses.append(
            """
            p.wallet IN (
                SELECT DISTINCT wallet
                FROM candidate_wallet_sources
                WHERE regime = ?
            )
            """.strip()
        )
        params.append(regime)

    if start_ts is not None:
        clauses.append("p.entry_time >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append("p.entry_time <= ?")
        params.append(end_ts)

    if min_trades > 0:
        clauses.append(
            """
            p.wallet IN (
                SELECT wallet
                FROM positions_reconstructed
                WHERE status = 'CLOSED' AND version = ?
                GROUP BY wallet
                HAVING COUNT(*) >= ?
            )
            """.strip()
        )
        params.extend([version, min_trades])

    rows = connection.execute(
        f"""
        SELECT
            p.id AS position_id,
            p.wallet,
            p.market_id,
            p.outcome,
            p.entry_price,
            p.size,
            p.entry_time,
            p.exit_time,
            p.duration,
            p.pnl,
            p.status,
            pm.mfe,
            pm.mae,
            pm.time_to_mfe,
            CASE
                WHEN pm.time_to_mfe IS NOT NULL AND p.duration IS NOT NULL
                THEN MAX(0, p.duration - pm.time_to_mfe)
                ELSE NULL
            END AS time_after_mfe,
            entry_event.asset_id,
            tec.spread,
            tec.imbalance,
            tec.price_change_30s,
            tec.volatility_30s,
            (
                SELECT CAST(te_prev.price AS REAL)
                FROM trade_events te_prev
                WHERE te_prev.asset_id = entry_event.asset_id
                  AND te_prev.timestamp < p.entry_time
                  AND te_prev.timestamp >= p.entry_time - 60
                ORDER BY te_prev.timestamp DESC, te_prev.id DESC
                LIMIT 1
            ) AS prev_trade_price_60s,
            (
                SELECT COUNT(*)
                FROM trade_events te_recent
                WHERE te_recent.asset_id = entry_event.asset_id
                  AND te_recent.timestamp BETWEEN p.entry_time - 60 AND p.entry_time
            ) AS recent_trade_count_60s
        FROM positions_reconstructed p
        JOIN trade_events entry_event
            ON entry_event.id = p.entry_trade_event_id
        LEFT JOIN position_path_metrics pm
            ON pm.position_id = p.id
        LEFT JOIN trade_entry_context tec
            ON tec.trade_event_id = p.entry_trade_event_id
        WHERE {' AND '.join(clauses)}
        ORDER BY p.entry_time, p.id
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _passes_momentum_trigger(row: dict, config: StrategyConfig) -> bool:
    entry_price = float(row["entry_price"] or 0.0)
    price_change_30s = row.get("price_change_30s")
    volatility_30s = row.get("volatility_30s")
    spread = row.get("spread")
    imbalance = row.get("imbalance")

    if price_change_30s is not None:
        abs_move = abs(float(price_change_30s or 0.0))
    else:
        prev_trade_price = row.get("prev_trade_price_60s")
        if prev_trade_price is None:
            return False
        abs_move = abs(entry_price - float(prev_trade_price))

    recent_trade_count = int(row.get("recent_trade_count_60s") or 0)
    if not (config.min_abs_price_change <= abs_move <= config.max_abs_price_change):
        return False
    if recent_trade_count < config.min_recent_trade_count:
        return False

    if spread is not None and float(spread) > config.max_spread:
        return False
    if volatility_30s is not None and float(volatility_30s) < config.min_volatility_30s:
        return False
    if imbalance is not None and abs(float(imbalance) - 0.5) < config.min_imbalance_edge:
        return False
    return True


def apply_strategy(rows: list[dict], config: StrategyConfig) -> list[SimulatedTrade]:
    trades: list[SimulatedTrade] = []
    for row in rows:
        entry_price = float(row["entry_price"] or 0.0)
        if not (config.min_entry_price <= entry_price <= config.max_entry_price):
            continue
        trigger_passed = _passes_momentum_trigger(row, config)
        if not trigger_passed:
            continue

        duration = int(row["duration"] or 0)
        size = float(row["size"] or 0.0)
        mfe = float(row["mfe"] or 0.0)
        time_to_mfe = int(row["time_to_mfe"]) if row["time_to_mfe"] is not None else None
        raw_time_after_mfe = row.get("time_after_mfe")
        time_after_mfe = (
            int(raw_time_after_mfe)
            if raw_time_after_mfe is not None
            else max(0, duration - time_to_mfe)
            if time_to_mfe is not None
            else duration
        )
        mae = float(row["mae"]) if row["mae"] is not None else None

        stop_loss_value = config.stop_loss_fraction * entry_price * size

        if mfe <= 0 or time_to_mfe is None:
            simulated_pnl = max(mae if mae is not None else -stop_loss_value, -stop_loss_value)
            simulated_pnl -= config.slippage_per_trade
            effective_duration = min(max(duration, config.min_signal_seconds), config.late_signal_seconds)
            trades.append(
                SimulatedTrade(
                    position_id=int(row["position_id"]),
                    wallet=str(row["wallet"]),
                    market_id=str(row["market_id"]),
                    outcome=str(row["outcome"]),
                    entry_price=entry_price,
                    size=size,
                    entry_time=int(row["entry_time"]),
                    duration=duration,
                    effective_duration=effective_duration,
                    actual_pnl=float(row["pnl"] or 0.0),
                    simulated_pnl=simulated_pnl,
                    pnl_multiplier=0.0,
                    mfe=mfe,
                    mae=mae,
                    time_to_mfe=time_to_mfe,
                    time_after_mfe=time_after_mfe,
                    trigger_passed=trigger_passed,
                )
            )
            continue

        if time_to_mfe < config.min_signal_seconds:
            if not config.include_sub_60s:
                continue
            capture_ratio = config.strong_capture_ratio * config.aggressive_early_penalty_ratio
        elif time_to_mfe <= config.strong_signal_seconds:
            capture_ratio = config.strong_capture_ratio
        elif time_to_mfe <= config.late_signal_seconds:
            capture_ratio = config.late_capture_ratio
        else:
            capture_ratio = config.weak_capture_ratio

        if config.name == "strict_timing" and not (
            config.min_signal_seconds <= time_to_mfe <= config.strong_signal_seconds
        ):
            continue

        simulated_pnl = capture_ratio * mfe * config.execution_success_rate
        if time_after_mfe > config.heavy_hold_penalty_seconds:
            simulated_pnl -= config.heavy_penalty_ratio * mfe
        elif time_after_mfe > config.moderate_hold_penalty_seconds:
            simulated_pnl -= config.moderate_penalty_ratio * mfe

        if config.name == "soft_exit":
            simulated_pnl *= config.soft_exit_ratio

        simulated_pnl -= config.slippage_per_trade

        effective_duration = min(
            duration if config.include_sub_60s else max(duration, config.min_signal_seconds),
            max(config.late_signal_seconds, config.strong_signal_seconds),
        )

        trades.append(
            SimulatedTrade(
                position_id=int(row["position_id"]),
                wallet=str(row["wallet"]),
                market_id=str(row["market_id"]),
                outcome=str(row["outcome"]),
                entry_price=entry_price,
                size=size,
                entry_time=int(row["entry_time"]),
                duration=duration,
                effective_duration=effective_duration,
                actual_pnl=float(row["pnl"] or 0.0),
                simulated_pnl=simulated_pnl,
                pnl_multiplier=capture_ratio,
                mfe=mfe,
                mae=mae,
                time_to_mfe=time_to_mfe,
                time_after_mfe=time_after_mfe,
                trigger_passed=trigger_passed,
            )
        )
    return trades


def compute_metrics(
    rows: list[dict],
    trades: list[SimulatedTrade],
    *,
    variant: str,
    wallet: str | None,
    regime: str,
) -> StrategySimulationResult:
    total_trades = len(rows)
    trades_used = len(trades)

    if not trades:
        return StrategySimulationResult(
            variant=variant,
            wallet=wallet,
            regime=regime,
            total_trades=total_trades,
            trades_used=0,
            win_rate=0.0,
            avg_pnl=0.0,
            total_pnl=0.0,
            pnl_per_trade=0.0,
            pnl_per_minute=0.0,
            max_drawdown=0.0,
            pnl_series=(),
            trades=(),
        )

    ordered = sorted(trades, key=lambda trade: (trade.entry_time, trade.position_id))
    total_pnl = sum(trade.simulated_pnl for trade in ordered)
    wins = sum(1 for trade in ordered if trade.simulated_pnl > 0)
    avg_pnl = total_pnl / trades_used
    effective_minutes = sum(max(1, trade.effective_duration) for trade in ordered) / 60.0
    pnl_per_minute = total_pnl / effective_minutes if effective_minutes > 0 else 0.0

    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    pnl_series: list[tuple[int, float, float]] = []
    for trade in ordered:
        cumulative += trade.simulated_pnl
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
        pnl_series.append((trade.entry_time, trade.simulated_pnl, cumulative))

    return StrategySimulationResult(
        variant=variant,
        wallet=wallet,
        regime=regime,
        total_trades=total_trades,
        trades_used=trades_used,
        win_rate=wins / trades_used if trades_used > 0 else 0.0,
        avg_pnl=avg_pnl,
        total_pnl=total_pnl,
        pnl_per_trade=total_pnl / trades_used if trades_used > 0 else 0.0,
        pnl_per_minute=pnl_per_minute,
        max_drawdown=max_drawdown,
        pnl_series=tuple(pnl_series),
        trades=tuple(ordered),
    )


def simulate(
    *,
    wallet: str | None = None,
    regime: str = "global",
    variant: str = "baseline",
    min_trades: int = 0,
    start_ts: int | None = None,
    end_ts: int | None = None,
    version: int = 1,
    settings: Settings | None = None,
) -> StrategySimulationResult:
    settings = settings or get_settings()
    config = get_strategy_config(variant)
    with managed_connection(settings) as connection:
        rows = load_positions(
            connection,
            wallet=wallet,
            regime=regime,
            min_trades=min_trades,
            start_ts=start_ts,
            end_ts=end_ts,
            version=version,
        )
    trades = apply_strategy(rows, config)
    return compute_metrics(rows, trades, variant=variant, wallet=wallet, regime=regime)


def render_strategy_report(result: StrategySimulationResult) -> str:
    if result.total_pnl > 0 and result.win_rate >= 0.5:
        conclusion = "Edge appears profitable in the simulated execution window."
    elif result.trades_used == 0:
        conclusion = "No trades matched the current filter."
    else:
        conclusion = "Edge does not appear consistently profitable under this rule set."

    lines = [
        "=== STRATEGY RESULTS ===",
        f"Variant: {result.variant}",
        f"Regime: {result.regime}",
    ]
    if result.wallet:
        lines.append(f"Wallet: {result.wallet}")
    lines.extend(
        [
            "",
            f"Trades analyzed: {result.total_trades}",
            f"Trades used: {result.trades_used}",
            "",
            f"Win rate: {result.win_rate:.1%}",
            f"Avg PnL: {result.avg_pnl:+.4f}",
            f"Total PnL: {result.total_pnl:+.4f}",
            "",
            f"PnL/trade: {result.pnl_per_trade:+.4f}",
            f"PnL/min: {result.pnl_per_minute:+.4f}",
            f"Max drawdown: {result.max_drawdown:+.4f}",
            "",
            f"Conclusion: {conclusion}",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate extracted TIP V1 strategy rules.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--variant", default="baseline", choices=sorted(VARIANT_CONFIGS), help="Strategy variant to simulate.")
    parser.add_argument("--wallet", help="Optional single wallet filter.")
    parser.add_argument("--regime", default="global", help="Regime to simulate.")
    parser.add_argument("--min-trades", type=int, default=0, help="Minimum closed trades per wallet.")
    parser.add_argument("--start-ts", type=int, help="Filter to positions entered at or after this UNIX timestamp.")
    parser.add_argument("--end-ts", type=int, help="Filter to positions entered at or before this UNIX timestamp.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    result = simulate(
        wallet=args.wallet,
        regime=args.regime,
        variant=args.variant,
        min_trades=args.min_trades,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        version=args.version,
        settings=settings,
    )
    print(render_strategy_report(result))


if __name__ == "__main__":
    main()
