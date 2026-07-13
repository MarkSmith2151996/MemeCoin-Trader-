# Window 1 Fixture Manifest

Use this checklist before manually supplying a window-1 fixture pair. It is not
a data collection form and must not be stored as a live fixture.

## Batch Identity

| Field | Value to record manually |
| --- | --- |
| Candidate label | `<candidate-label>-window-1` |
| Candidate path | `<candidate-label>-window-1.json` |
| Sample label | `<sample-label>-window-1` |
| Sample path | `<sample-label>-window-1.json` |
| UTC window start | `<YYYY-MM-DDTHH:MM:SSZ>` |
| UTC window end | `<YYYY-MM-DDTHH:MM:SSZ>` |

Both `FixtureCheckRequest` values must use the same UTC start/end instants.
The two JSON files must each be a top-level array and are supplied manually.

## Completeness Checklist

- [ ] Candidate and sample labels are nonblank.
- [ ] Candidate and sample paths are explicit and refer to separate files.
- [ ] Both UTC window values use ISO-8601 `Z` timestamps.
- [ ] Each record includes `source`, `type`, a nonblank `mint_address`, and a
      timezone-aware `observed_at` inside the inclusive window.
- [ ] Each `WHALE_TRACKER` record has a nonblank
      `payload.tracked_wallet.label`.
- [ ] No fixture has been created, modified, or persisted through this project.
- [ ] `check_json_signal_fixtures()` returned `complete` for the paired
      requests before comparison.

## Gate Result

Record exactly one external manual outcome:

- `complete`: run `compare_json_signal_fixtures()` on the same explicit paths.
- `malformed`, `incomplete`, `unmatched_window`, or `inconclusive`: stop; do
  not compare, collect through this project, or edit the wallet configuration.

## After A Complete Check

Record neutral totals, unique/novel mints, duplicates and rate, source mix, and
wallet/source origins in external notes. A single window does not change
`config/wallets_to_track.yaml` or support price, PnL, outcome, policy, or trade
claims.
