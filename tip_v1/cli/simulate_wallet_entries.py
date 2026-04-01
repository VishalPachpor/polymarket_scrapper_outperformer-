from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection


TAKE_PROFIT = "TAKE_PROFIT"
STOP_LOSS = "STOP_LOSS"
TIME_EXIT = "TIME_EXIT"
NO_PATH = "NO_PATH"


@dataclass(frozen=True)
class WalletEntryExitConfig:
    take_profit: float = 0.05
    stop_loss: float = 0.02
    max_hold_seconds: int = 180
    entry_delay_seconds: int = 0


@dataclass(frozen=True)
class WalletEntryTradeSimulation:
    position_id: int
    wallet: str
    market_id: str
    asset_id: str
    outcome: str
    entry_time: int
    entry_price: float
    simulated_entry_time: int
    simulated_entry_price: float
    size: float
    actual_pnl: float
    simulated_pnl: float
    exit_type: str
    exit_time: int
    exit_price: float
    hold_seconds: int


@dataclass(frozen=True)
class WalletEntrySimulationResult:
    wallet: str
    total_trades: int
    simulated_trades: int
    win_rate: float
    avg_pnl: float
    total_pnl: float
    pnl_per_trade: float
    avg_hold_seconds: float
    take_profit_rate: float
    stop_loss_rate: float
    time_exit_rate: float
    no_path_rate: float
    actual_wallet_pnl: float
    pnl_delta: float
    entry_delay_seconds: int
    trades: tuple[WalletEntryTradeSimulation, ...]


def _first_price_at_or_after(samples: list[tuple[int, float]], *, target_ts: int) -> tuple[int, float] | None:
    for timestamp, price in samples:
        if timestamp >= target_ts:
            return (timestamp, price)
    return None


def _load_wallet_entries(
    connection,
    *,
    wallet: str,
    version: int,
) -> list[dict]:
    rows = connection.execute(
        """
        SELECT
            p.id AS position_id,
            p.wallet,
            p.market_id,
            p.outcome,
            p.entry_price,
            p.size,
            p.entry_time,
            p.exit_time,
            p.pnl,
            entry_event.asset_id AS asset_id
        FROM positions_reconstructed AS p
        JOIN trade_events AS entry_event
          ON entry_event.id = p.entry_trade_event_id
        WHERE p.wallet = ?
          AND p.version = ?
          AND p.status = 'CLOSED'
          AND p.pnl IS NOT NULL
          AND entry_event.asset_id IS NOT NULL
        ORDER BY p.entry_time, p.id
        """,
        (wallet, version),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_price_path(
    connection,
    *,
    asset_id: str,
    entry_time: int,
    end_time: int,
) -> list[tuple[int, float]]:
    rows = connection.execute(
        """
        SELECT timestamp, price
        FROM market_price_history
        WHERE asset_id = ?
          AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp, id
        """,
        (asset_id, entry_time, end_time),
    ).fetchall()
    return [(int(row["timestamp"]), float(row["price"])) for row in rows]


def simulate_wallet_entries(
    *,
    wallet: str,
    settings: Settings | None = None,
    version: int = 1,
    config: WalletEntryExitConfig | None = None,
) -> WalletEntrySimulationResult:
    settings = settings or get_settings()
    config = config or WalletEntryExitConfig()

    with managed_connection(settings) as connection:
        entries = _load_wallet_entries(connection, wallet=wallet, version=version)
        simulations: list[WalletEntryTradeSimulation] = []
        actual_wallet_pnl = 0.0

        for row in entries:
            position_id = int(row["position_id"])
            asset_id = str(row["asset_id"])
            market_id = str(row["market_id"])
            outcome = str(row["outcome"])
            entry_time = int(row["entry_time"])
            entry_price = float(row["entry_price"])
            size = float(row["size"])
            actual_pnl = float(row["pnl"])
            actual_wallet_pnl += actual_pnl

            delayed_entry_time = entry_time + config.entry_delay_seconds
            end_time = delayed_entry_time + config.max_hold_seconds
            path = _load_price_path(
                connection,
                asset_id=asset_id,
                entry_time=entry_time,
                end_time=end_time,
            )

            exit_type = NO_PATH
            exit_time = end_time
            exit_price = entry_price
            simulated_entry_time = delayed_entry_time
            simulated_entry_price = entry_price

            if path:
                if config.entry_delay_seconds > 0:
                    delayed_entry = _first_price_at_or_after(path, target_ts=delayed_entry_time)
                    if delayed_entry is not None:
                        simulated_entry_time, simulated_entry_price = delayed_entry
                        delayed_path = [
                            (timestamp, price)
                            for timestamp, price in path
                            if timestamp >= simulated_entry_time
                        ]
                    else:
                        delayed_path = []
                else:
                    delayed_path = path

                tp_price = simulated_entry_price * (1.0 + config.take_profit)
                sl_price = simulated_entry_price * (1.0 - config.stop_loss)

                for timestamp, price in delayed_path:
                    if price >= tp_price:
                        exit_type = TAKE_PROFIT
                        exit_time = timestamp
                        exit_price = price
                        break
                    if price <= sl_price:
                        exit_type = STOP_LOSS
                        exit_time = timestamp
                        exit_price = price
                        break

                if delayed_path and exit_type == NO_PATH:
                    timed = _first_price_at_or_after(delayed_path, target_ts=end_time)
                    if timed is not None:
                        exit_time, exit_price = timed
                    else:
                        exit_time, exit_price = delayed_path[-1]
                    exit_type = TIME_EXIT

            simulated_pnl = (exit_price - simulated_entry_price) * size
            simulations.append(
                WalletEntryTradeSimulation(
                    position_id=position_id,
                    wallet=wallet,
                    market_id=market_id,
                    asset_id=asset_id,
                    outcome=outcome,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    simulated_entry_time=simulated_entry_time,
                    simulated_entry_price=simulated_entry_price,
                    size=size,
                    actual_pnl=actual_pnl,
                    simulated_pnl=simulated_pnl,
                    exit_type=exit_type,
                    exit_time=exit_time,
                    exit_price=exit_price,
                    hold_seconds=max(0, exit_time - simulated_entry_time),
                )
            )

    total_trades = len(simulations)
    wins = sum(1 for trade in simulations if trade.simulated_pnl > 0)
    total_pnl = sum(trade.simulated_pnl for trade in simulations)
    exit_counts = {
        TAKE_PROFIT: sum(1 for trade in simulations if trade.exit_type == TAKE_PROFIT),
        STOP_LOSS: sum(1 for trade in simulations if trade.exit_type == STOP_LOSS),
        TIME_EXIT: sum(1 for trade in simulations if trade.exit_type == TIME_EXIT),
        NO_PATH: sum(1 for trade in simulations if trade.exit_type == NO_PATH),
    }

    return WalletEntrySimulationResult(
        wallet=wallet,
        total_trades=total_trades,
        simulated_trades=total_trades,
        win_rate=(wins / total_trades) if total_trades else 0.0,
        avg_pnl=(total_pnl / total_trades) if total_trades else 0.0,
        total_pnl=total_pnl,
        pnl_per_trade=(total_pnl / total_trades) if total_trades else 0.0,
        avg_hold_seconds=(sum(trade.hold_seconds for trade in simulations) / total_trades) if total_trades else 0.0,
        take_profit_rate=(exit_counts[TAKE_PROFIT] / total_trades) if total_trades else 0.0,
        stop_loss_rate=(exit_counts[STOP_LOSS] / total_trades) if total_trades else 0.0,
        time_exit_rate=(exit_counts[TIME_EXIT] / total_trades) if total_trades else 0.0,
        no_path_rate=(exit_counts[NO_PATH] / total_trades) if total_trades else 0.0,
        actual_wallet_pnl=actual_wallet_pnl,
        pnl_delta=total_pnl - actual_wallet_pnl,
        entry_delay_seconds=config.entry_delay_seconds,
        trades=tuple(simulations),
    )


def render_wallet_entry_simulation_report(
    result: WalletEntrySimulationResult,
    *,
    compare_wallet_pnl: bool = False,
) -> str:
    lines = [
        f"Wallet Entry Simulation: {result.wallet}",
        f"Trades Simulated: {result.simulated_trades}",
        f"Win Rate: {result.win_rate:.2%}",
        f"Avg PnL: {result.avg_pnl:+.4f}",
        f"Total PnL: {result.total_pnl:+.4f}",
        f"PnL Per Trade: {result.pnl_per_trade:+.4f}",
        f"Avg Time In Trade: {result.avg_hold_seconds:.1f}s",
        f"Entry Delay: {result.entry_delay_seconds}s",
        "",
        "Exit Distribution:",
        f"- take_profit: {result.take_profit_rate:.2%}",
        f"- stop_loss: {result.stop_loss_rate:.2%}",
        f"- time_exit: {result.time_exit_rate:.2%}",
        f"- no_path: {result.no_path_rate:.2%}",
    ]
    if compare_wallet_pnl:
        lines.extend(
            [
                "",
                f"Wallet PnL: {result.actual_wallet_pnl:+.4f}",
                f"Simulated PnL: {result.total_pnl:+.4f}",
                f"Delta: {result.pnl_delta:+.4f}",
            ]
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate wallet entries with controlled exits.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--wallet", required=True, help="Wallet address to simulate.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--take-profit", type=float, default=0.05, help="Take-profit threshold as decimal return.")
    parser.add_argument("--stop-loss", type=float, default=0.02, help="Stop-loss threshold as decimal return.")
    parser.add_argument("--max-hold", type=int, default=180, help="Maximum hold in seconds.")
    parser.add_argument("--entry-delay", type=int, default=0, help="Delay simulated entry by N seconds.")
    parser.add_argument(
        "--compare-wallet-pnl",
        action="store_true",
        help="Include original wallet realized PnL in the report.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)
    result = simulate_wallet_entries(
        wallet=args.wallet,
        settings=settings,
        version=args.version,
        config=WalletEntryExitConfig(
            take_profit=args.take_profit,
            stop_loss=args.stop_loss,
            max_hold_seconds=args.max_hold,
            entry_delay_seconds=args.entry_delay,
        ),
    )
    print(
        render_wallet_entry_simulation_report(
            result,
            compare_wallet_pnl=args.compare_wallet_pnl,
        )
    )


if __name__ == "__main__":
    main()
