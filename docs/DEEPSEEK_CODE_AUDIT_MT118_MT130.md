## DeepSeek/OpenCode Audit: MT-118 through MT-130

### Range And Assumptions

- Assumed DeepSeek/OpenCode executor span: MT-118 through MT-130.
- Reviewed commits from `5cdbe82` (MT-118) through `9dac80a` (MT-129), with `9283cdf` (MT-130 review note) included for context.
- Exact per-commit model attribution was not available in git history, so this audit uses the recent task range requested in MT-131.

### Overall Result

- WARN

### Findings

#### High

1. Archived paper rows are still counted by soak capacity/accounting.

- Files: `src/cli.py:361`, `src/cli.py:507`
- `run_bounded_paper_cycle()` records `initial_open_positions` and `persisted_open_positions` with raw SQL `status != 'CLOSED'` counts.
- Archived legacy rows remain `OPEN` in the denormalized table columns, so these raw counts still treat them as active even though `PositionManager.get_all_open()` excludes them.
- Operational evidence: after `paper-state --archive-legacy --confirm` archived 5 paper rows, `paper-state`, `paper-pnl`, and `paper-report` all showed `0` open paper positions and `5` archived exclusions, but the next `paper-soak` still logged `starting_open_positions=5`, `persisted_open_positions=5`, and 12 capacity blocks.
- Impact: post-archive soak diagnostics are not fully trustworthy, and operators can be told capacity is exhausted when reporting surfaces say the paper book is empty.

#### Medium

1. Embedded live-readiness inside `paper-report` is weaker than the standalone `live-readiness` command.

- Files: `src/cli.py:2073-2084`, `src/cli.py:2716`
- The standalone `live-readiness` command injects provider instances plus a circuit breaker and position manager into `evaluate_micro_live_readiness()`.
- `paper-report` calls `evaluate_micro_live_readiness(settings)` without those dependencies, so it can report `wallet_balance_lookup_unavailable`, `position_reconciliation_unavailable`, and `circuit_breaker_unavailable` even when the standalone command shows more specific and more accurate diagnostics such as `insufficient_wallet_balance`, `no_live_positions_to_reconcile`, and `paper_mode_unaffected`.
- Impact: report output is safe and fail-closed, but not fully truthful relative to the richer readiness path already available in the same CLI.

### Audit Answers

- Can any paper command mutate live positions by accident?
No direct evidence found. `paper-close` rejects non-paper positions, `close_paper_positions()` only iterates `mode == "paper"`, and the archive workflow only targets legacy paper rows.

- Can legacy/mock paper rows still be reported as reliable PnL?
Default report/PnL paths now prevent that. Legacy rows are labeled `legacy_unknown` and downgraded to low-confidence/unavailable PnL.

- Can PnL be calculated from fabricated/default prices?
Future fills no longer invent `1.0` entry prices. Legacy rows can still exist historically, but the new reporting path no longer presents them as reliable PnL.

- Are DB migrations backward compatible and idempotent?
Mostly yes. The SQLite `close_price_sol` migration is idempotent, and newer position state (`fill_quality`, `archived`, archive metadata) is backward-compatible because it lives in the persisted JSON blob with conservative defaults. The main maintenance risk is that some raw SQL paths still reason about table columns instead of the JSON-backed state.

- Are network calls isolated from tests/faked where required?
Yes in the recent paper-reporting span. Tests use fake providers or stub classes for price paths. Existing DexScreener provider coverage uses mocked HTTP behavior.

- Are mark-provider failures safely diagnosed instead of crashing?
Yes. Provider failures degrade to diagnostic reasons such as `no_pairs`, `provider_timeout`, `provider_error`, and `malformed_response`.

- Are CLI outputs truthful and not overstating PnL or readiness?
PnL output is conservative. Readiness output is only partially truthful because `paper-report` uses a weaker readiness call than the standalone command.

- Are live guardrails still fail-closed?
Yes from the audited surface. Live remains disabled by default and readiness still reports arming failures.

- Did any task enable live trading or weaken arming conditions?
No evidence found.

- Did any task add, print, log, or commit secrets/private key material?
No evidence found.

- Did any task loosen risk checks or allow AI to bypass risk?
No evidence found.

- Are tests meaningful, or are there shallow tests that miss important regressions?
The paper-reporting tests are generally meaningful and improved over the span. The main blind spot was the archived-row soak-capacity path, which was not covered.

- Are there hidden coupling/maintenance risks from recent changes?
Yes. Position state is increasingly modeled in the JSON payload while some operational counters still query legacy scalar columns directly. That split caused the archived-row mismatch.

### Blocking Issues

- None for continued paper-only development.

### Non-Blocking Cleanup Recommendations

1. Make soak capacity/accounting derive open-position counts from `PositionManager` or otherwise honor archived rows.
2. Reuse the richer standalone readiness dependency wiring inside `paper-report` so embedded readiness matches the main command.
3. Add one regression test that archives legacy paper rows, runs the soak path, and asserts archived rows are not counted as active capacity.

### Validation

- `git status --short`
- `git log --oneline -25`
- `git show --stat --oneline HEAD~15..HEAD`
- `python3 -m pytest tests -q` -> `480 passed`
- `python3 -m src.cli env-readiness` -> safe fail-closed, no secrets
- `python3 -m src.cli live-readiness` -> `NOT READY`, live still disabled
- `python3 -m src.cli paper-state` -> `0` open paper, `5` archived exclusions
- `python3 -m src.cli paper-pnl` -> zero-open simulated report
- `python3 -m src.cli paper-pnl --marks live` -> zero-open stable report
- `python3 -m src.cli paper-report` -> zero-open report with archived exclusions
- `python3 -m src.cli paper-report --marks live` -> zero-open report with archived exclusions and hints

### Safety Confirmation

- Live trading remains disabled by default.
- No private key or secret material was added, printed, or committed in the audited span.
