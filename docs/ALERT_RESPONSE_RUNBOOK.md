# V2 Alert Response Runbook

Use this runbook for the V2 executor and data collector only. Run commands from
the repository root. Keep `EXECUTION_MODE=paper` unless an approved, observed
micro-live procedure explicitly requires otherwise.

Before responding to an alert, capture the current service state and dashboard:

```bash
systemctl status memecoin-executor.service memecoin-data.service --no-pager
python3 scripts/v2_dashboard.py --once
journalctl -u memecoin-executor.service -n 100 --no-pager
```

Do not delete the executor lock, halt marker, or circuit-breaker file to make an
alert disappear. Preserve the alert text, mint, timestamps, and runtime-event
details before taking a recovery action.

## Quarantined Live Position

**Alert:** `Live position quarantined after sell failures`

The executor exhausted the 300, 500, and 1000 bps live-exit attempts. It keeps
the Hive position quarantined, removes only that mint from monitoring and
capacity, and blocks re-entry for the mint. Other positions remain managed.

1. Keep the current execution mode unchanged and do not restart solely to free
   capacity.
2. Record the mint, failed slippage attempts, and `position_quarantined`
   runtime event from the executor journal and dashboard.
3. Independently verify the wallet token balance and the matching Hive row. A
   sell is not complete until the wallet-clear check succeeds.
4. If an operator-approved full live shutdown is necessary, run
   `python3 scripts/kill_switch.py`. It stops/verifies the executor first,
   latches paper mode, and retries unsettled live rows. This can submit real
   sells, so it is not a paper drill command.
5. After the wallet and Hive state are reconciled, inspect the breaker with
   `python3 scripts/reset_breaker.py`. Reset it only after the failure is
   understood: `python3 scripts/reset_breaker.py --confirm MANUAL_RESET`.

`reset_breaker.py` does not clear `LIVE_KILL_SWITCH` or authorize live mode.

## Material Wallet-Only Holding

**Alert:** `Live wallet-only holdings`

The wallet has a non-dust token holding that is not an expected open position
and is not already quarantined. The executor records
`wallet_only_holdings_monitor_only`, blocks new live entries, and continues to
manage known positions. It does not assume ownership or liquidate the unknown
token.

1. Do not sell, delete, or add a Hive position based only on the alert.
2. Record the mint, raw balance, estimated SOL value, and transaction history.
3. Determine whether the holding belongs to a prior approved trade, another
   application, an airdrop, or a reconciliation defect.
4. Reconcile the ownership evidence with Hive before any recovery action.
5. Keep the executor monitor-only until the discrepancy is resolved and the
   next startup reconciliation is clean. A separate approved liquidation is
   required for an independently owned token.

## PumpPortal Feed Stale

**Alert:** `PumpPortal feed stale: Jupiter fallback active`

The global PumpPortal feed is stale. New entries are blocked while the executor
uses Jupiter marks for held positions for up to 90 seconds. Existing exits still
run from the fallback marks.

1. Confirm the executor remains active and the dashboard heartbeat advances.
2. Check that the executor journal reports the 90-second Jupiter fallback and
   that held positions continue receiving marks.
3. Do not restart the executor during the grace period; that would discard the
   evidence needed to distinguish recovery from escalation.
4. If `PumpPortal feed recovered` arrives before the grace expires, record the
   duration and resume normal observation.
5. If fallback produces no held-token marks or the full grace expires, follow
   the emergency-halt response below.

## Emergency Halt (Exit 42)

**Signals:** `Memecoin executor emergency close`, an
`emergency_close_all` runtime event, `/tmp/memecoin-executor-halted`, or an
executor exit status of `42`.

Exit 42 is intentionally excluded from systemd restart. The executor writes the
halt marker, attempts to close its positions, records the event, and stops for
manual review.

1. Do not clear the halt marker or start the executor immediately.
2. Preserve the executor journal and dashboard/runtime-event evidence. Verify
   each open or recently closed position against the applicable paper or wallet
   state.
3. Correct the underlying cause, such as a sustained feed failure or unavailable
   fallback prices, before recovery.
4. After review, remove only `/tmp/memecoin-executor-halted`, inspect the
   circuit breaker, and restart the executor in paper mode with
   `sudo -n systemctl start memecoin-executor.service`.
5. Confirm an active service, fresh heartbeat, and clean dashboard snapshot
   before considering any later human-gated procedure.

## Singleton-Lock Contention (Exit 43)

**Signal:** `FATAL: another memecoin executor instance is running` with exit
status `43`.

Exit 43 is retryable lock contention, not an emergency. Systemd uses
`Restart=on-failure`, so the unit normally retries after its configured delay.

1. Do not delete `/tmp/memecoin_executor.lock`; the advisory file lock protects
   the wallet and Hive entry/close paths.
2. Inspect `memecoin-executor.service` and the host for the expected single
   executor process. Do not terminate an unknown process without establishing
   its owner and purpose.
3. If an old executor is confirmed, stop it through its owning service or
   supervised process, then allow systemd to start the canonical unit.
4. If repeated retries reach the systemd start limit, resolve the duplicate
   process first, then explicitly start `memecoin-executor.service`.
5. Confirm there is one executor, an advancing heartbeat, and no exit 42 halt
   marker. No breaker reset is required for exit 43 alone.

## Closeout

For every incident, record the alert title, UTC time, services state, positions
affected, wallet/Hive reconciliation result, and the exact recovery action.
Keep `EXECUTION_MODE=paper` after drills and incidents until a separate,
approved live procedure is observed.
