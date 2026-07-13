# Candidate Wallet Fixture Template

Use this template only for a manually supplied, bounded fixture pair. It does
not collect signals, modify tracked wallets, or authorize a trade.

## Matched Window Metadata

Record this metadata outside each JSON file. Both the candidate and sample
fixture must use the identical UTC window.

| Field | Candidate | Sample comparator |
| --- | --- | --- |
| Label | `<candidate-label>` | `<sample-label>` |
| Window start | `2026-07-12T12:00:00Z` | `2026-07-12T12:00:00Z` |
| Window end | `2026-07-12T12:10:00Z` | `2026-07-12T12:10:00Z` |
| Fixture path | `<candidate>.json` | `<sample>.json` |

Pass the label, explicit path, and UTC start/end as a `FixtureCheckRequest`.
Do not add window metadata to the JSON file: the fixture loader requires a
top-level array.

## JSON Fixture

Replace every angle-bracket value before using the file. Every `observed_at`
value must be timezone-aware UTC and fall inside the recorded inclusive window.

```json
[
  {
    "source": "WHALE_TRACKER",
    "type": "BUY",
    "mint_address": "<SOLANA_MINT_ADDRESS>",
    "observed_at": "2026-07-12T12:03:15Z",
    "payload": {
      "tracked_wallet": {
        "label": "<candidate-label>"
      }
    }
  }
]
```

For a non-whale record, `source`, `type`, `mint_address`, and `observed_at`
are still required. A `WHALE_TRACKER` record additionally requires a nonblank
`payload.tracked_wallet.label`.

## Manual Gate

1. Build one candidate file and one sample-comparator file for one identical
   bounded UTC window.
2. Run `check_json_signal_fixtures()` first. Stop unless it returns `complete`.
3. Run `compare_json_signal_fixtures()` only on the same explicit paths.
4. Keep the neutral comparison results in external manual notes. Do not persist
   them through this project or edit `config/wallets_to_track.yaml`.
