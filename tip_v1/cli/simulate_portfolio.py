from __future__ import annotations

import argparse
from dataclasses import dataclass

from tip_v1.cli.simulate_wallet_entries import (
    NO_PATH,
    STOP_LOSS,
    TAKE_PROFIT,
    TIME_EXIT,
    WalletEntryExitConfig,
    WalletEntryTradeSimulation,
    simulate_wallet_entries,
)
from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database


@dataclass(frozen=True)
class PortfolioSimulationResult:
    wallets: tuple[str, ...]
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


def simulate_portfolio(
    *,
    wallets: list[str] | tuple[str, ...],
    settings: Settings | None = None,
    version: int = 1,
    config: WalletEntryExitConfig | None = None,
) -> PortfolioSimulationResult:
    settings = settings or get_settings()
    config = config or WalletEntryExitConfig()
    ordered_wallets = tuple(wallets)

    all_trades: list[WalletEntryTradeSimulation] = []
    actual_wallet_pnl = 0.0

    for wallet in ordered_wallets:
        wallet_result = simulate_wallet_entries(
            wallet=wallet,
            settings=settings,
            version=version,
            config=config,
        )
        all_trades.extend(wallet_result.trades)
        actual_wallet_pnl += wallet_result.actual_wallet_pnl

    all_trades.sort(key=lambda trade: (trade.entry_time, trade.position_id))
    total_trades = len(all_trades)
    wins = sum(1 for trade in all_trades if trade.simulated_pnl > 0)
    total_pnl = sum(trade.simulated_pnl for trade in all_trades)
    exit_counts = {
        TAKE_PROFIT: sum(1 for trade in all_trades if trade.exit_type == TAKE_PROFIT),
        STOP_LOSS: sum(1 for trade in all_trades if trade.exit_type == STOP_LOSS),
        TIME_EXIT: sum(1 for trade in all_trades if trade.exit_type == TIME_EXIT),
        NO_PATH: sum(1 for trade in all_trades if trade.exit_type == NO_PATH),
    }

    return PortfolioSimulationResult(
        wallets=ordered_wallets,
        total_trades=total_trades,
        simulated_trades=total_trades,
        win_rate=(wins / total_trades) if total_trades else 0.0,
        avg_pnl=(total_pnl / total_trades) if total_trades else 0.0,
        total_pnl=total_pnl,
        pnl_per_trade=(total_pnl / total_trades) if total_trades else 0.0,
        avg_hold_seconds=(sum(trade.hold_seconds for trade in all_trades) / total_trades) if total_trades else 0.0,
        take_profit_rate=(exit_counts[TAKE_PROFIT] / total_trades) if total_trades else 0.0,
        stop_loss_rate=(exit_counts[STOP_LOSS] / total_trades) if total_trades else 0.0,
        time_exit_rate=(exit_counts[TIME_EXIT] / total_trades) if total_trades else 0.0,
        no_path_rate=(exit_counts[NO_PATH] / total_trades) if total_trades else 0.0,
        actual_wallet_pnl=actual_wallet_pnl,
        pnl_delta=total_pnl - actual_wallet_pnl,
        entry_delay_seconds=config.entry_delay_seconds,
        trades=tuple(all_trades),
    )


def render_portfolio_simulation_report(
    result: PortfolioSimulationResult,
    *,
    compare_wallet_pnl: bool = False,
) -> str:
    lines = [
        "=== PORTFOLIO SIMULATION ===",
        f"Wallets: {len(result.wallets)}",
        f"Trades: {result.simulated_trades}",
        f"Win Rate: {result.win_rate:.2%}",
        f"Total PnL: {result.total_pnl:+.4f}",
        f"Avg PnL: {result.avg_pnl:+.4f}",
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
    parser = argparse.ArgumentParser(description="Simulate a portfolio of wallet entries with controlled exits.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--wallets", nargs="+", required=True, help="Wallet addresses to simulate.")
    parser.add_argument("--version", type=int, default=1, help="Reconstruction version.")
    parser.add_argument("--take-profit", type=float, default=0.05, help="Take-profit threshold as decimal return.")
    parser.add_argument("--stop-loss", type=float, default=0.02, help="Stop-loss threshold as decimal return.")
    parser.add_argument("--max-hold", type=int, default=180, help="Maximum hold in seconds.")
    parser.add_argument("--entry-delay", type=int, default=0, help="Delay simulated entry by N seconds.")
    parser.add_argument(
        "--compare-wallet-pnl",
        action="store_true",
        help="Include aggregate original wallet realized PnL in the report.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)
    result = simulate_portfolio(
        wallets=args.wallets,
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
        render_portfolio_simulation_report(
            result,
            compare_wallet_pnl=args.compare_wallet_pnl,
        )
    )


if __name__ == "__main__":
    main()
