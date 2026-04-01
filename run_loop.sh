#!/bin/bash

set -u

ROOT_DIR="/Users/vishalpatil/Desktop/Polymarket-scrapper"
DB_PATH="$ROOT_DIR/tip_v1/tip_v1.sqlite3"
LOG_DIR="$ROOT_DIR/logs"

mkdir -p "$LOG_DIR"

cd "$ROOT_DIR" || exit 1

while true
do
  echo "==============================" | tee -a "$LOG_DIR/run_loop.log"
  echo "RUN: $(date)" | tee -a "$LOG_DIR/run_loop.log"
  echo "==============================" | tee -a "$LOG_DIR/run_loop.log"

  python3 -m tip_v1.discovery.orchestrator --db-path "$DB_PATH" 2>&1 | tee -a "$LOG_DIR/run_loop.log"
  python3 -m tip_v1.cli.monitor --db-path "$DB_PATH" 2>&1 | tee -a "$LOG_DIR/run_loop.log"

  echo "Sleeping 10 minutes..." | tee -a "$LOG_DIR/run_loop.log"
  sleep 600
done
