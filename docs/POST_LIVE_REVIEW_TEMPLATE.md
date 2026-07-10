# Post-Live Review

> Complete this report immediately after each guarded micro-live smoke test.
> This template documents what happened, what was learned, and whether to proceed.

## 1. Test Metadata

| Field | Value |
|---|---|
| Test date/time | |
| Operator | |
| Git commit | |
| Command sequence used (from runbook) | |

## 2. Pre-Live State

### env-readiness

```
(paste output of `python3 -m src.cli env-readiness`)
```

### live-readiness

```
(paste output of `python3 -m src.cli live-readiness`)
```

### paper-soak (before live)

| Metric | Value |
|---|---|
| Signals scanned | |
| Paper trades entered | |
| Risk rejections | |
| Source failures | |

```
(paste output of `python3 -m src.cli paper-soak --max-signals 50 --timeout-seconds 180`)
```

## 3. Trade Details

| Field | Value |
|---|---|
| Token/mint tested | |
| Intended SOL size | |
| Buy command used | |
| Sell command used | |
| Buy tx signature | |
| Sell tx signature | |
| Observed slippage (%) | |

### Fees

| Fee type | Amount (SOL) |
|---|---|
| Priority fee | |
| Jito tip (if applicable) | |
| Total fee | |

## 4. Wallet State

| Metric | Before (SOL) | After (SOL) |
|---|---|---|
| Wallet balance | | |

## 5. Position State

### DB positions before live

| Mint | Amount | Status |
|---|---|---|
| | | |

### DB positions after live

| Mint | Amount | Status |
|---|---|---|
| | | |

### Reconciliation result

```
(paste output of live-readiness or reconciliation check after trade)
```

## 6. Circuit Breaker State

| Check | Value |
|---|---|
| Breaker state before | |
| Breaker state after | |
| RPC failures | |
| Simulation failures | |
| Submission failures | |
| Health check freshness | |

## 7. Logs Review

| Check | Pass/Fail |
|---|---|
| No secrets (API keys) in logs | |
| No private keys in logs | |
| No raw tx payloads in logs | |
| No .env contents in logs | |

## 8. Issues Found

| Issue | Severity | Resolution |
|---|---|---|
| | | |

## 9. Go/No-Go Decision

**Decision:** GO / NO-GO

If NO-GO, list what must be fixed before next attempt:

1. 
2. 
3. 

## 10. Rollback / Kill-Switch Confirmation

| Check | Confirmed |
|---|---|
| LIVE_KILL_SWITCH restored to `true` | |
| execution.mode restored to `paper` | |
| Wallet balance reconciled | |
| Positions reconciled | |
| Circuit breaker reset (if needed) | |
| `.env` secrets rotated (if leaked) | |

## 11. Notes

(Anything unexpected, learned, or worth flagging for the next test.)
