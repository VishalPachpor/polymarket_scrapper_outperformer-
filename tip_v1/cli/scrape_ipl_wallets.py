from __future__ import annotations

import argparse
import csv
import sys
from decimal import Decimal
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.discovery.ipl_market_scanner import IPLMarket, scan_ipl_markets
from tip_v1.discovery.ipl_wallet_collector import (
    WalletIPLStats,
    aggregate_wallet_stats,
)


def _to_float(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.0001"))) if isinstance(value, Decimal) else float(value)


def _filter_markets(
    markets: list[IPLMarket],
    *,
    only_resolved: bool,
    min_volume: float,
    market_limit: int | None,
) -> list[IPLMarket]:
    filtered = [m for m in markets if m.volume_num >= min_volume]
    if only_resolved:
        filtered = [m for m in filtered if m.closed]
    filtered.sort(key=lambda m: m.volume_num, reverse=True)
    if market_limit is not None:
        filtered = filtered[:market_limit]
    return filtered


def _write_csv(
    output_path: Path,
    wallet_stats: dict[str, WalletIPLStats],
    *,
    min_pnl: float,
    min_trades: int,
    profitable_only: bool,
) -> int:
    rows = []
    for wallet, stats in wallet_stats.items():
        total_pnl = stats.realized_pnl + stats.unrealized_pnl
        win_rate = (
            stats.winning_position_count / stats.closed_position_count
            if stats.closed_position_count > 0
            else 0.0
        )
        roi = (
            float(total_pnl) / float(stats.matched_buy_cost + stats.open_inventory_cost)
            if (stats.matched_buy_cost + stats.open_inventory_cost) > 0
            else 0.0
        )
        if profitable_only and total_pnl <= 0:
            continue
        if stats.trade_count < min_trades:
            continue
        if float(total_pnl) < min_pnl:
            continue
        rows.append(
            {
                "wallet": wallet,
                "pseudonym": stats.pseudonym,
                "total_pnl_usdc": _to_float(total_pnl),
                "realized_pnl_usdc": _to_float(stats.realized_pnl),
                "unrealized_pnl_usdc": _to_float(stats.unrealized_pnl),
                "roi": round(roi, 4),
                "win_rate": round(win_rate, 4),
                "closed_positions": stats.closed_position_count,
                "winning_positions": stats.winning_position_count,
                "trade_count": stats.trade_count,
                "buy_count": stats.buy_count,
                "sell_count": stats.sell_count,
                "ipl_markets_traded": len(stats.markets_traded),
                "total_volume_usdc": _to_float(stats.total_volume_usdc),
                "matched_buy_cost_usdc": _to_float(stats.matched_buy_cost),
                "open_inventory_cost_usdc": _to_float(stats.open_inventory_cost),
                "open_inventory_value_usdc": _to_float(stats.open_inventory_value),
                "first_trade_ts": stats.first_trade_ts or "",
                "last_trade_ts": stats.last_trade_ts or "",
            }
        )

    rows.sort(key=lambda r: r["total_pnl_usdc"], reverse=True)
    fieldnames = list(rows[0].keys()) if rows else [
        "wallet", "pseudonym", "total_pnl_usdc", "realized_pnl_usdc",
        "unrealized_pnl_usdc", "roi", "win_rate", "closed_positions",
        "winning_positions", "trade_count", "buy_count", "sell_count",
        "ipl_markets_traded", "total_volume_usdc", "matched_buy_cost_usdc",
        "open_inventory_cost_usdc", "open_inventory_value_usdc",
        "first_trade_ts", "last_trade_ts",
    ]
    with output_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scrape Polymarket wallets active in IPL markets and rank by profitability.",
    )
    parser.add_argument("--output", type=Path, default=Path("ipl_profitable_wallets.csv"))
    parser.add_argument(
        "--include-open",
        action="store_true",
        help="Include unresolved IPL markets (PnL on open inventory marked at last trade price).",
    )
    parser.add_argument(
        "--only-resolved",
        action="store_true",
        help="Restrict scan to closed/resolved IPL markets only (most accurate PnL).",
    )
    parser.add_argument("--min-market-volume", type=float, default=0.0)
    parser.add_argument("--market-limit", type=int, default=None)
    parser.add_argument("--min-pnl", type=float, default=0.0)
    parser.add_argument("--min-trades", type=int, default=1)
    parser.add_argument(
        "--profitable-only",
        action="store_true",
        default=True,
        help="Only output wallets with total_pnl > 0 (default true).",
    )
    parser.add_argument(
        "--all-wallets",
        action="store_true",
        help="Output every wallet, not just profitable ones.",
    )
    args = parser.parse_args(argv)

    settings = get_settings()

    print("Scanning IPL events from gamma API ...", flush=True)
    include_closed = True
    if args.include_open and not args.only_resolved:
        include_closed = True
    markets = scan_ipl_markets(settings=settings, include_closed=include_closed)
    print(f"Found {len(markets)} IPL markets total.", flush=True)

    only_resolved = args.only_resolved or not args.include_open
    filtered = _filter_markets(
        markets,
        only_resolved=only_resolved,
        min_volume=args.min_market_volume,
        market_limit=args.market_limit,
    )
    print(
        f"Scraping {len(filtered)} markets "
        f"(only_resolved={only_resolved}, min_volume={args.min_market_volume}, "
        f"limit={args.market_limit}).",
        flush=True,
    )
    if not filtered:
        print("No markets matched filters.", flush=True)
        return 1

    wallet_stats = aggregate_wallet_stats(filtered, settings=settings, progress=True)
    print(f"Collected {len(wallet_stats)} unique wallets across IPL markets.", flush=True)

    profitable_only = not args.all_wallets
    written = _write_csv(
        args.output,
        wallet_stats,
        min_pnl=args.min_pnl,
        min_trades=args.min_trades,
        profitable_only=profitable_only,
    )
    print(f"Wrote {written} rows to {args.output}.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
