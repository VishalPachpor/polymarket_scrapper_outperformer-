#!/bin/bash
# Runs the wallet signal engine + paper tracker continuously.
# This is the process that generates wallet_paper_signals and resolves outcomes.
# Run this alongside run_loop.sh (which handles discovery).
#
# Usage: ./run_tracker.sh [--log-level DEBUG]

set -u

ROOT_DIR="/Users/vishalpatil/Desktop/Polymarket-scrapper"
DB_PATH="$ROOT_DIR/tip_v1/tip_v1.sqlite3"
LOG_DIR="$ROOT_DIR/logs"
LOG_LEVEL="${1:-INFO}"

mkdir -p "$LOG_DIR"

cd "$ROOT_DIR" || exit 1

echo "==============================" | tee -a "$LOG_DIR/tracker.log"
echo "TRACKER START: $(date)" | tee -a "$LOG_DIR/tracker.log"
echo "==============================" | tee -a "$LOG_DIR/tracker.log"

echo "[pre-start] wallet_paper_signals: $(sqlite3 "$DB_PATH" 'SELECT COUNT(*) FROM wallet_paper_signals')" | tee -a "$LOG_DIR/tracker.log"
echo "[pre-start] wallet_paper_outcomes: $(sqlite3 "$DB_PATH" 'SELECT COUNT(*) FROM wallet_paper_outcomes')" | tee -a "$LOG_DIR/tracker.log"
echo "[pre-start] wallet_rankings: $(sqlite3 "$DB_PATH" 'SELECT COUNT(*) FROM wallet_rankings')" | tee -a "$LOG_DIR/tracker.log"

python3 -m tip_v1.wallet_engine.wallet_paper_tracker \
    --db-path "$DB_PATH" \
    --poll-interval 2 \
    --log-level "$LOG_LEVEL" \
    2>&1 | tee -a "$LOG_DIR/tracker.log"
