# Guarded Micro-Live Operator Runbook

This document defines the exact procedure for executing a guarded micro-live
smoke trade on the memecoin-trader system. The procedure is intentionally
conservative — a single tiny SOL buy, then a full exit, followed by complete
reconciliation and teardown.

## Required Environment Variables

Set these in `.env` (never commit `.env`):

| Variable | Purpose | Required for |
|---|---|---|
| `HELIUS_API_KEY` | RPC access for simulation, balance, holdings lookups | Transaction simulator, wallet balance, wallet holdings |
| `TRADING_WALLET_PUBLIC_KEY` | Read-only wallet address for balance/holdings checks | Wallet balance lookup, wallet holdings lookup |
| `TRADING_WALLET_PRIVATE_KEY` | Signing key for actual swap submission | Live buy / live exit execution (not readiness checks) |
| `LIVE_TRADING_ENABLED=true` | Arms live execution mode | Must be explicitly set |
| `LIVE_CONFIRMATION_PHRASE=I_UNDERSTAND_THIS_CAN_LOSE_REAL_SOL` | Confirms operator understands risk | Required by guardrails |
| `LIVE_KILL_SWITCH=false` | Disables the kill switch | Required by guardrails |
| `MAX_LIVE_TRADE_SOL=0.005` | Per-trade SOL cap (tiny) | Guardrails validation |
| `MAX_DAILY_LIVE_TRADES=1` | Daily trade count cap | Guardrails validation |
| `MAX_DAILY_LOSS_SOL=0.02` | Daily loss limit | Guardrails validation |
| `PRIMARY_RPC_URL` | Primary RPC endpoint (defaults to Helius) | Optional override |
| `BACKUP_RPC_URL` | Backup RPC endpoint | Optional override |

## Pre-Live Checklist

Every item must pass before proceeding to live testing:

1. **Repository state**
   - `git status` — clean (no uncommitted changes)
   - `.env` — no staged changes, contents reviewed for correctness

2. **Test suite**
   - `python3 -m pytest -q` — all tests pass

3. **Readiness gate**
   - `python3 -m src.cli live-readiness` — all checks report `ok`
   - No `not_ready` diagnostics
   - No `*_unavailable` diagnostics
   - No `*_failed` diagnostics

4. **Paper soak**
   - `python3 -m src.cli paper-soak --max-signals 50 --timeout-seconds 180 --mode discovery --fresh-evaluation-session`
   - No stale data warnings
   - No unexpected failures

5. **Wallet preparation** (see [WALLET_SETUP.md](WALLET_SETUP.md) for full guide)
   - Create a fresh disposable hot wallet (do not reuse personal wallet)
   - Fund wallet only with disposable SOL (e.g. ≤0.1 SOL)
   - No unrelated funds in the hot wallet
   - `TRADING_WALLET_PUBLIC_KEY` confirmed correct
   - `TRADING_WALLET_PRIVATE_KEY` only added **immediately before** the live smoke command
   - `LIVE_KILL_SWITCH=false` verified

6. **Secret safety**
   - `HELIUS_API_KEY` present
   - `TRADING_WALLET_PUBLIC_KEY` present
   - `TRADING_WALLET_PRIVATE_KEY` present only during active live window
   - No API keys, private keys, or raw tx payloads appear in `env-readiness` output

## Exact Command Sequence

### 1. Pre-flight

```bash
git status                              # must be clean
python3 -m pytest -q                    # all pass
python3 -m src.cli live-readiness       # all ok
python3 -m src.cli env-readiness        # all present (private key optional until live)
python3 -m src.cli paper-soak --max-signals 50 --timeout-seconds 180 --mode discovery --fresh-evaluation-session
```

### 2. Add private key

Add `TRADING_WALLET_PRIVATE_KEY` to `.env` only now
(see [WALLET_SETUP.md](WALLET_SETUP.md) for env progression guidance).

```bash
echo "TRADING_WALLET_PRIVATE_KEY=your_private_key_here" >> .env
python3 -m src.cli live-readiness       # confirm still READY
```

### 3. Micro-live smoke buy

```bash
python3 -m src.cli live-buy --mint <TOKEN_MINT_ADDRESS> --amount-sol 0.001
```

Verify:
- Exit code 0
- `"ok": true` in output
- `"tx_signature"` is a non-empty string (record it)

### 4. Post-buy verification

```bash
python3 -m src.cli live-readiness       # position_reconciliation should be ok or show reconciling state
```

### 5. Sell-only exit

```bash
python3 -m src.cli live-exit --mint <TOKEN_MINT_ADDRESS>
```

Verify:
- Exit code 0
- `"ok": true` in output
- `"tx_signature"` is a non-empty string (record it)

### 6. Post-live review

```bash
python3 -m src.cli live-readiness       # confirm back to clean state
git status                              # confirm clean
```

### 7. Restore kill switch & clear private key

```bash
sed -i 's/LIVE_KILL_SWITCH=false/LIVE_KILL_SWITCH=true/' .env
sed -i '/^TRADING_WALLET_PRIVATE_KEY=/d' .env
python3 -m src.cli live-readiness       # confirm NOT READY (expected, kill switch on)
```

## Abort Conditions

Abort immediately and do not proceed if any of these are true:

| Condition | Check |
|---|---|
| Any `NOT READY` item in live-readiness | `live-readiness` |
| Any stale data warning | `paper-soak` output |
| Any circuit breaker warning (`rpc_failure_*`, `simulation_*`, `submission_*`) | `live-readiness` circuit_breaker check |
| Wallet balance mismatch | `live-readiness` position_reconciliation |
| Simulator unavailable | `live-readiness` preflight diagnostics |
| Wallet holdings lookup unavailable | `live-readiness` position_reconciliation diagnostics |
| Wallet balance lookup unavailable | `live-readiness` preflight diagnostics |
| Git dirty | `git status` |
| Test failure | `python3 -m pytest -q` |
| Paper-soak failure | `paper-soak` exit code ≠ 0 |
| Unexpected `.env` change | Review `.env` diff |
| Any secret printed to logs | Review output of all commands |
| Any raw transaction payload printed | Review output of all commands |
| `env-readiness` shows unexpected secret values | `env-readiness` output |

If aborting:
1. Restore `LIVE_KILL_SWITCH=true` in `.env`
2. Remove `TRADING_WALLET_PRIVATE_KEY` from `.env`
3. Run `python3 -m src.cli live-readiness` to confirm `NOT READY`
4. Record the abort reason in a session log
5. File a bug/issue before retrying

## Post-Live Review Checklist

After the live test completes:

- [ ] Tx signature recorded for buy
- [ ] Tx signature recorded for sell
- [ ] Buy transaction confirmed landed on-chain
- [ ] Sell transaction confirmed landed on-chain
- [ ] DB position state reconciles with wallet holdings
- [ ] Logs contain no secrets (no private keys, API keys, raw tx payloads)
- [ ] `LIVE_KILL_SWITCH=true` restored
- [ ] `TRADING_WALLET_PRIVATE_KEY` removed/blanked from `.env`
- [ ] `git status` clean
- [ ] `STATUS.md` updated with test results
