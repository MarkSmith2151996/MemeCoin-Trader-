#!/usr/bin/env bash
# Runs one MT-721 replay from a durable external scheduler.
set -euo pipefail

start_date="$1"
end_date="$2"
output_dir="$3"
shift 3

exec /usr/bin/time -v /usr/bin/python3 scripts/capacity_sweep_bt_v2.py \
    --root /mnt/d/pumpapi-replay \
    --start "$start_date" \
    --end "$end_date" \
    --output-dir "$output_dir" \
    --repo-report "$output_dir/repo_report.md" \
    --progress-log "$output_dir/progress.log" \
    --price-ratio-p99 1.441798 \
    --price-ratio-p999 1022.511434 \
    --price-ratio-observations 315634867 \
    --mcap-floor 10000 \
    --mcap-ceiling 50000 \
    --min-age-seconds 22 \
    --max-age-seconds 1320 \
    --age-offset-seconds 39 \
    --txn-count-adjustment 1.24 \
    --min-volume-usd 500 \
    --min-volume-to-mcap-ratio 0.005 \
    --max-volume-to-mcap-ratio 50 \
    --min-buy-sell-ratio 0.5 \
    --min-pool-sol 100 \
    --creator-holdings-max 0 \
    --score-threshold-bonding 40 \
    --score-threshold-graduated 40 \
    --blocked-weekdays 2 \
    --blocked-hours-utc 0 19 20 21 \
    --hard-stop-pct 999 \
    --hard-stop-delay-seconds 30 \
    --trailing-stop-pct 2 \
    --trailing-arm-pct 2 \
    --take-profit-pct 150 \
    --time-stop-minutes 10 \
    --graduated-only \
    "$@"
