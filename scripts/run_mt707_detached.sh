#!/usr/bin/env bash
set -euo pipefail

ROOT="/mnt/d/pumpapi-replay"
OUT="$ROOT/results/mt707"
mkdir -p "$OUT"

# run-capped enforces the task's process-memory ceiling without touching services.
nohup run-capped 6G python3 scripts/mt707_tick_bar_reconciliation.py --root "$ROOT" "$@" \
  >"$OUT/mt707.progress.log" 2>&1 < /dev/null &
echo $! >"$OUT/mt707.pid"
echo "MT-707 started with PID $(cat "$OUT/mt707.pid")"
