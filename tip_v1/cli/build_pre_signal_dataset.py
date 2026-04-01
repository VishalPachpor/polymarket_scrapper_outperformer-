from __future__ import annotations

import argparse

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database
from tip_v1.pre_signal_engine.core import (
    build_pre_signal_dataset,
    build_pre_signal_dataset_report,
    write_pre_signal_dataset_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a labeled pre-wallet signal dataset from trade-event windows.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--output", default="pre_signal_dataset.csv", help="Path to write the dataset CSV.")
    parser.add_argument("--window", type=int, default=10, help="Lookback window in seconds.")
    parser.add_argument(
        "--prediction-horizon",
        type=int,
        default=10,
        help="Negative windows must have no target-wallet trade in the next N seconds.",
    )
    parser.add_argument("--limit", type=int, default=10_000, help="Maximum positive examples to include.")
    parser.add_argument(
        "--negative-ratio",
        type=float,
        default=1.0,
        help="How many negative examples to include per positive example.",
    )
    parser.add_argument("--wallets", nargs="+", help="Optional wallet addresses to target explicitly.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    initialize_database(settings)
    result = build_pre_signal_dataset(
        settings=settings,
        wallets=args.wallets,
        window_seconds=args.window,
        prediction_horizon_seconds=args.prediction_horizon,
        limit=args.limit,
        negative_ratio=args.negative_ratio,
    )
    output_path = write_pre_signal_dataset_csv(result.rows, output_path=args.output)
    print(
        build_pre_signal_dataset_report(
            result,
            output_path=output_path,
            window_seconds=args.window,
            prediction_horizon_seconds=args.prediction_horizon,
        )
    )


if __name__ == "__main__":
    main()

