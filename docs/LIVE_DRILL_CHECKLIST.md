# V2 Live Drill Checklist

Run these drills only after the live wallet, Hive access, and alert credentials have
been independently reviewed. Keep `EXECUTION_MODE=paper` unless the micro-live step
explicitly says otherwise.

## 1. Kill Drill

1. Seed paper-safe V2 test positions in a disposable Hive environment.
2. Confirm `memecoin-executor.service` is stopped, then run `python3 scripts/kill_switch.py`.
3. Confirm every unsettled live Hive row is closed only after its wallet balance clears.
4. Re-run the command with `EXECUTION_MODE=paper` to prove an incomplete previous kill is retried.
5. If `systemctl` is unavailable, the command must report a free singleton lock or refuse; use `--force` only after manually proving the executor is stopped.

Expected logs: `confirmed memecoin-executor.service is stopped before liquidation`,
`V2 KILL SWITCH SELL OK`, or `V2 kill switch incomplete` for a retained row.

## 2. Restart Drill

1. Start the executor with open V2 paper positions and blocked live arming conditions.
2. Confirm startup logs `Started in monitor-only mode` and the open positions remain hydrated.
3. Force a hard-stop mark and confirm the position closes while entries stay blocked.
4. Seed an unsettled live Hive row, set the environment to paper, and restart the executor.
5. Confirm it refuses startup with `paper startup refused while live Hive/wallet state remains`.
6. Resolve the live state, or use `MEMECOIN_ALLOW_ORPHANED_LIVE_STATE=true` only for an explicitly reviewed recovery run.

## 3. Telegram Delivery Drill

1. Configure the real Telegram credentials in the untracked `.env` and restart the bot.
2. Send `kill switch` from the owner chat while the executor service is stopped.
3. Verify the reply contains the V2 kill-switch summary, not a legacy SQLite report.
4. Trigger one expected critical alert in the disposable environment and confirm the configured delivery channel receives it.

Expected logs: `V2 KILL SWITCH SELL OK`, `Live wallet-only holdings`, and
`PumpPortal feed stale: Jupiter fallback active` when those conditions are simulated.

## 4. Feed-Fallback Drill

1. Hold a disposable paper position and simulate a global PumpPortal silence.
2. Confirm `PumpPortal global feed stale; using Jupiter fallback marks for 90s`.
3. Confirm Jupiter fallback marks continue evaluating hard stops during the grace period.
4. Restore the feed and confirm `PumpPortal global feed recovered` before 90 seconds.
5. In a separate disposable run, leave the feed stale for the full grace period and confirm the executor writes the halt marker and exits 42.

## 5. Micro-Live Roundtrip

1. Complete every gate in `docs/MICRO_LIVE_RUNBOOK.md` and explicitly arm live mode.
2. Use exactly one `0.001 SOL` buy/sell roundtrip with the approved mint and operator present.
3. Confirm the sell has a confirmed signature, wallet token clearance is at or below 10 raw units, and the Hive position is closed with the actual fill.
4. Run reconciliation after the close; it must accept the same <=10-raw-unit residue while rejecting a tracked material mismatch.
5. Restore paper mode, trip the kill switch if required, inspect the breaker, and record the result in `docs/POST_LIVE_REVIEW_TEMPLATE.md`.

Expected logs: `LIVE BUY RESULT`, `LIVE SELL BALANCE ... cleared`,
`closed mint=...`, and no `startup reconciliation mismatch`.
