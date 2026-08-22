#!/usr/bin/env python3
"""Sweep MIN_EVAL_AGE_S at 0, 30, 60, 120 over 30 days with MAX_OPEN=5."""
import sys, time
sys.path.insert(0, '/home/dev/projects/memecoin-trader/scripts')

START = '2026-07-22'
END = '2026-08-21'
MAX_OPEN = 5
MIN_AGES = [0, 30, 60, 120]

import capacity_sweep as base
from pathlib import Path

_orig_passes_gates = base.passes_gates

results = []
for min_age in MIN_AGES:
    def make_patched(floor):
        def patched(**kwargs):
            age = kwargs.get('age_seconds')
            if age is not None and age < floor:
                return False
            return _orig_passes_gates(**kwargs)
        return patched

    base.passes_gates = make_patched(min_age)

    root = Path('/mnt/d/pumpapi-replay')
    enriched_dir = root / 'derived' / 'enriched'
    all_dates = base.parquet_dates(enriched_dir, None, None)
    dates = base.parquet_dates(enriched_dir, START, END)
    config = base.ConfigState(max_open=MAX_OPEN)

    print(f'\n=== MIN_AGE={min_age}s, {len(dates)} days, MAX_OPEN={MAX_OPEN} ===', flush=True)
    t0 = time.time()
    base.replay_all(dates, all_dates, root, enriched_dir,
                    base.load_sol_prices(root / 'derived'), [config])
    elapsed = time.time() - t0

    trades = config.trades
    total_entries = len(trades)
    total_wins = sum(t.pnl_sol > 0 for t in trades)
    total_pnl = sum(t.pnl_sol for t in trades)
    total_friction = sum(base.friction_pnl(t) for t in trades)
    wr = (total_wins / total_entries * 100) if total_entries else 0
    daily_mean = total_friction / len(dates) if dates else 0

    results.append((min_age, total_entries, wr, total_pnl, total_friction, daily_mean, elapsed))
    print(f'  entries={total_entries} wr={wr:.1f}% pnl={total_pnl:+.2f} friction={total_friction:+.2f} daily_mean={daily_mean:+.3f} ({elapsed:.0f}s)', flush=True)

base.passes_gates = _orig_passes_gates

print('\n=== SUMMARY ===')
print(f'{"min_age":>8} {"entries":>8} {"win_rate":>8} {"raw_pnl":>10} {"fric_pnl":>10} {"daily_avg":>10}')
for min_age, entries, wr, pnl, friction, daily, elapsed in results:
    print(f'{min_age:>7}s {entries:>8} {wr:>7.1f}% {pnl:>+10.2f} {friction:>+10.2f} {daily:>+10.3f}')
