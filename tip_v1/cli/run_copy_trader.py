from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from tip_v1.copy_trader.copier import CopyTraderConfig, run_copy_trader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the smart-money copy-trader. Polls watchlist wallets, "
                    "alerts via Telegram, optionally fires CLOB orders.",
    )
    parser.add_argument("--poll-interval", type=float, default=None,
                        help="Seconds between polling cycles (default: env COPY_TRADER_POLL_INTERVAL or 1.5)")
    parser.add_argument("--fetch-limit", type=int, default=None,
                        help="Trades fetched per wallet per poll (default: 20)")
    parser.add_argument("--db-path", type=Path, default=None,
                        help="SQLite DB for tx-hash dedupe (default: tip_v1/tip_v1.sqlite3)")
    parser.add_argument("--enable-execution", action="store_true",
                        help="Enable CLOB execution (still dry-run unless --live).")
    parser.add_argument("--live", action="store_true",
                        help="Disable dry-run — actually place orders. Requires --enable-execution.")
    parser.add_argument("--only-buys", action="store_true",
                        help="Phase-1 mode: only alert on opens, ignore sells.")
    parser.add_argument("--min-notional", type=float, default=None,
                        help="Skip trades smaller than this USDC notional (default: $25)")
    parser.add_argument("--max-cycles", type=int, default=None,
                        help="Stop after N polling cycles (default: run forever).")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    base = CopyTraderConfig.from_env()
    config = CopyTraderConfig(
        poll_interval_seconds=args.poll_interval if args.poll_interval is not None else base.poll_interval_seconds,
        trades_api_url=base.trades_api_url,
        fetch_limit=args.fetch_limit if args.fetch_limit is not None else base.fetch_limit,
        dry_run=not args.live,
        enable_execution=args.enable_execution or base.enable_execution,
        only_buys=args.only_buys or base.only_buys,
        min_notional_usdc=args.min_notional if args.min_notional is not None else base.min_notional_usdc,
    )

    if args.live and not config.enable_execution:
        print("--live requires --enable-execution", file=sys.stderr)
        return 2
    if args.live:
        print("⚠️  LIVE MODE — real orders will be placed. Press Ctrl-C within 5s to abort.", flush=True)
        import time
        time.sleep(5)

    try:
        run_copy_trader(config=config, db_path=args.db_path, max_cycles=args.max_cycles)
    except KeyboardInterrupt:
        print("\nstopped.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
