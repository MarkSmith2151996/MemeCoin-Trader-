# Memecoin Decision Ledger Patterns

## Purpose

The proposed `memecoin_decision` ledger is an observability and learning record for why a candidate was seen, rejected, entered, exited, or later labeled. It is not a strategy engine, an approval path, a wallet store, or a live-trading control plane.

This document records constraints for a future implementation only. It does not add a database, migration, runtime write, or tool.

## Existing State To Preserve

The current runtime state is an async SQLite database initialized by `src/core/database.py`:

- `trades` stores paper or live-mode trade records, including execution metadata.
- `positions` stores paper or live-mode positions, partial exits, fill quality, and realized PnL fields.
- `paper_decisions` stores bounded paper decision telemetry and a JSON diagnostic payload.
- `paper_soak_runs` stores bounded paper-cycle summaries.

Paper decision persistence occurs after a paper cycle has evaluated candidates. Rejected snapshots are a normalized allowlist under `diagnostics_json.recheck_snapshot`; they intentionally exclude raw provider responses, headers, credentials, transactions, and other execution artifacts. Later-mark and outcome-label CLI commands are read-only diagnostics over those snapshots. They require a positive persisted rejection baseline before requesting a later mark, and retain `inconclusive` when either observation is unavailable.

The SQLite state remains the current source of truth for paper execution, positions, PnL, paper-cycle telemetry, and existing CLI reports. A future ledger must not replace it, change its keys, backfill invented values, or make its writes dependent on Postgres availability.

## Hive And Custodian Patterns

The relevant Hive/Custodian patterns are:

- Keep operational entities relational and give each persisted row a named downstream reader or decision-review use.
- Use a dedicated PostgreSQL schema such as `memecoin_decision`, rather than mixing application rows into Custodian system schemas.
- Route agent access through narrow purpose-built tools. Agents and planners should not receive database credentials or broad write SQL access.
- Split capabilities by role: controlled application/tool writers and read-only planner-facing queries.
- Introduce migrations incrementally, with explicit schema ownership, stable identifiers, idempotent writes, and bounded verification before any read cutover.
- Treat JSON as a bounded evidence envelope, not an unreviewed raw-provider archive. Normalize fields used for filters, joins, or aggregates into columns.
- Preserve evidence provenance, observation time, provider status, and explicit unknown states. Do not infer success, provider failure categories, or missing values from absence alone.

The Hive migration experience also argues against a premature dual-write integration here: mirroring must be introduced only after an independently useful ledger schema and narrow writer contract exist, with failure handling that cannot block safety-critical paper or live paths.

## Ledger Consumers

The ledger should support bounded answers to questions such as:

- Why was a mint seen, and which normalized sources contributed?
- Which risk check first blocked a decision, and which checks were `PASS`, `FAIL`, or `UNKNOWN`?
- What normalized provider snapshot and provenance existed at decision time?
- Which paper entry, exit, position, or exact attribution link is associated with a decision?
- Was a rejected-candidate outcome measurable, or did baseline/later-mark evidence remain unavailable?
- Which diagnostic labels and parameter-review records were produced without claiming a strategy improvement?

Each future table and field needs a consumer from one of these bounded history, rejection-review, trade-reasoning, missed-winner, or learning-progress views. Data with no consumer should not be persisted.

## Allowed Writers

Future writers should be limited to narrow, validated operations:

- A decision writer records one normalized decision and its immutable evidence references after the existing runtime has made its decision.
- A trade-entry or trade-exit writer records an already-created paper trade or a safely redacted future live record; it never submits a trade.
- A rejection-outcome writer records an observed baseline, later mark, or label only with its provider, timestamp, and explicit availability state.
- A backfill/import writer may copy existing SQLite paper telemetry only when it preserves original identifiers and marks legacy or missing fields as unknown.

Writers must not modify risk configuration, candidate score, ranking, acceptance, position size, execution state, fill data, PnL, attribution status, guardrails, circuit-breaker state, or live readiness. They must not store private keys, RPC URLs, authorization headers, serialized transactions, raw provider payloads, or secrets.

## Out Of Scope

The future ledger must not:

- become a second execution database or replace the SQLite paper state;
- change risk gates or turn a diagnostic record into a pass, trade, or live-ready claim;
- infer PnL, attribution, provider health, or performance without linked evidence;
- permit direct agent SQL, general mutation endpoints, or arbitrary JSON blobs;
- create live trading, hold wallet secrets, or submit transactions;
- use outcome labels as training labels, ranking inputs, source suppression, or policy changes without a separate reviewed task.

## Migration And Compatibility Risks

- Current `paper_decisions` is denormalized and stores the authoritative record in `record_json`, with selected fields and diagnostics duplicated for CLI reads. Future import code must preserve the original decision ID and timestamp and avoid treating duplicated fields as independently authoritative.
- Snapshot coverage is partial. Legacy records can lack `recheck_snapshot`, normalized check outcomes, rejection liquidity, or a rejection baseline. Missing evidence must import as missing or `UNKNOWN`, never as a negative observation.
- Current trades and positions contain paper and live modes in the same tables. A ledger must retain mode on every related entity and never join same-mint records across modes without an explicit relation.
- PnL confidence depends on fill quality and usable marks. A ledger cannot claim realized or unrealized PnL merely because a trade or mark row exists.
- Existing SQLite writes use `INSERT OR REPLACE`; a future append-oriented evidence ledger needs explicit idempotency keys and a documented correction/supersession policy so replacement does not erase history.
- A future Postgres outage, migration failure, or ledger validation failure must not block existing SQLite paper persistence or alter runtime results until an explicitly tested cutover is approved.

## Safe Next Step

The next design task may specify a normalized `memecoin_decision` schema and narrow tool contracts. The first implementation slice, if separately approved, should be a diagnostic-only migration plus a small read/write prototype using synthetic or explicitly imported paper evidence, with no runtime integration.
