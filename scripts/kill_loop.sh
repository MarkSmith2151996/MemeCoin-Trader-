#!/usr/bin/env bash
# Stop the memecoin strategy and prevent the cron watchdog from restarting it.

set -u

KILLSWITCH="/tmp/memecoin_killswitch"
touch "$KILLSWITCH"
echo "Created $KILLSWITCH"

# Preserve unrelated cron jobs while removing only memecoin watchdog entries.
cron_tmp=$(mktemp)
if crontab -l 2>/dev/null | grep -viE 'memecoin|watchdog_memecoin' >"$cron_tmp"; then
    crontab "$cron_tmp"
    echo "Removed memecoin cron entries"
else
    echo "No memecoin cron entries found"
fi
rm -f "$cron_tmp"

for pattern in 'watchdog_memecoin.sh' 'scripts/run_strategy_b.py' 'python3 scripts/run_strategy_b.py'; do
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        kill -9 $pids 2>/dev/null || true
    fi
done

sleep 1
if pgrep -f 'watchdog_memecoin.sh|scripts/run_strategy_b.py' >/dev/null 2>&1; then
    echo "ERROR: memecoin processes still running"
    pgrep -af 'watchdog_memecoin.sh|scripts/run_strategy_b.py' || true
    exit 1
fi

echo "Verified: 0 watchdog/strategy processes running"
