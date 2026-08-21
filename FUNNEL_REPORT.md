# Funnel Diagnostic — discovery to entry (MT-609)

- Sample window: 2026-08-21T13:24:33.981258+00:00 → 2026-08-21T14:24:34.216258+00:00 UTC
- Polls: 3476 (1s cadence)

## Stage 1 — Jupiter poll

| Metric | Count |
| --- | ---: |
| Raw candidate responses (poll events) | 113,758 |
| Avg candidates per poll | 32.7 |
| Unique new mints | 1,951 |
| Dedup skips (already evaluated this hour) | 8,938 |
| Watch-list pending updates | 0 |

Jupiter surfaces the same ~33 tokens every poll; only 1,951 mints were genuinely new across the hour.

## Stage 2 — Watch list admission (new mints only)

| Metric | Count | Share of new mints |
| --- | ---: | ---: |
| New mints | 1,951 | 100.0% |
| Admitted to watch list (<2 min old) | 1,944 | 99.6% |
| Screened immediately (not admitted) | 21 | 1.1% |

Rejection reasons:

- `age>=120s`: 21

## Stage 3 — Watch list evaluation

| Metric | Count |
| --- | ---: |
| Evaluated at 2-min age threshold (fresh data) | 1,851 |
| Expired — Jupiter dropped before 2 min (screened on last data) | 14 |

Both paths still run `screen_coin`; the expired path screens on stale cached activity data.

## Stage 4 — Gate screening

| Gate | Passed | % of evaluated |
| --- | ---: | ---: |
| Passed age (0-22m) | 1,886 | 100.0% |
| Passed mcap ($5.1K-$50K) | 204 | 10.8% |
| Passed pool depth (>=5 SOL) | 198 | 10.5% |
| Passed strength score (>=40) | 197 | 10.4% |
| Passed txns (age-adjusted) | 198 | 10.5% |
| Passed volume (>=$500) | 187 | 9.9% |
| Passed vol/mcap (0.005-50) | 197 | 10.4% |
| Low-fee check (warn or pass) | 1,886 | 100.0% |
| Passed buy/sell (>=0.5) | 198 | 10.5% |
| Passed RugCheck | 198 | 10.5% |
| Passed holder concentration | 193 | 10.2% |
| Full screen pass | 183 | 9.7% |

First failing gate on rejected evaluations:

- `mcap`: 1,682 (89.2%)
- `volume`: 10 (0.5%)
- `liquidity`: 6 (0.3%)
- `holder`: 4 (0.2%)
- `score`: 1 (0.1%)

## Stage 5 — Entry attempt

| Outcome | Count | % of full pass |
| --- | ---: | ---: |
| Full screen pass | 183 | 100.0% |
| Blocked by MAX_OPEN capacity | 105 | 57.4% |
| Reached try_enter | 78 | 42.6% |
| Blocked — repeat_loser ban | 1 | 0.5% |
| Blocked — weekday/hour gate | 0 | 0.0% |
| Blocked — no valid price | 3 | 1.6% |
| Blocked — pool too thin at entry | 0 | 0.0% |
| Blocked — swap failed/None | 0 | 0.0% |
| Blocked — zero-token fill | 0 | 0.0% |
| Blocked — position already open | 2 | 1.1% |
| Blocked — record/open failure | 0 | 0.0% |
| **Entered** | **72** | **39.3%** |

## Stage 6 — Where the 3,487 → 535 drop happens

The Strategy BT replay enters **3,487** trades on 2026-08-20 from 4,349,721 enriched rows (MT-608); the live loop recorded **535** closed positions that day. This funnel run shows the live loop's own throughput:

| Stage | Count | Biggest single drop |
| --- | ---: | --- |
| Unique new mints/hour | 1,951 | |
| Admitted to watch list | 1,944 | |
| Evaluated | 1,886 | |
| Full screen pass | 183 | |
| Entered | 72 | |

**Biggest single drop: gate screening — 1,703 tokens lost.**

Gate-level breakdown of the screening drop:

| Gate | Passed | Drop from prior gate |
| --- | ---: | ---: |
| Passed age (0-22m) | 1,886 | 0 |
| Passed mcap ($5.1K-$50K) | 204 | 1,682 |
| Passed pool depth (>=5 SOL) | 198 | 6 |
| Passed strength score (>=40) | 197 | 1 |
| Passed txns (age-adjusted) | 198 | -1 |
| Passed volume (>=$500) | 187 | 11 |
| Passed vol/mcap (0.005-50) | 197 | -10 |
| Passed buy/sell (>=0.5) | 198 | -1 |
| Passed RugCheck | 198 | 0 |
| Passed holder concentration | 193 | 5 |
| Full pass | 183 | 10 |

## Caveats

- Backtest numbers are the MT-608 replay reference; the live funnel is this sample hour only, and the live loop's gates differ from the replay where RugCheck/holder data is unavailable in the archive (see BT_ALIGNMENT_DIFF.md).
- `already_seen` and `watch_pending` are poll-event counts (the same mint reappears every poll); all other counters are distinct mints.
- This is diagnostic only — no gates, parameters, or trading logic were changed.
