# Memecoin Decision Ledger Schema

## Status And Scope

This is a proposed PostgreSQL design for a future `memecoin_decision` schema. It is diagnostic-only: no migration, connection, runtime writer, MCP tool, or SQLite behavior is added by this document.

The current SQLite database remains the source of truth for paper execution, positions, and existing CLI reports. The proposed ledger records normalized evidence after an existing runtime event; it never decides, authorizes, submits, or changes an event.

## Conventions

- IDs are UUIDs or preserved source IDs; all timestamps are UTC `timestamptz`.
- `mode` is required on every decision, trade, and position relation (`paper` or a future guarded `live` record).
- `*_json` values are bounded, allowlisted JSONB evidence envelopes. Raw provider payloads, keys, URLs containing credentials, headers, serialized transactions, and arbitrary blobs are forbidden.
- `unknown`, unavailable, missing, and not-collected evidence remains explicit. `NULL` never means a failed risk check or a successful provider call.
- Append immutable observations. Corrections use a new row with `supersedes_*_id`, not an overwrite.
- Foreign keys describe evidence links, not execution dependencies. Ledger write failure must not block current SQLite persistence or runtime behavior.

## Core Tables

### `memecoin_decision.decisions`

Central record for an observed candidate decision. Consumer: coin history and trade-reasoning views.

| Field | Proposed type | Notes |
| --- | --- | --- |
| `decision_id` | UUID PK | Idempotency key; preserve `paper_decisions.id` during an import. |
| `mint_address` | text | Required Solana mint string. |
| `symbol`, `name` | text nullable | Normalized only when observed. |
| `decision_type` | text | `seen`, `rejected`, `accepted`, `entry`, `exit`, or `labeled`; constrained enum/check in a migration. |
| `decision_time`, `created_at` | timestamptz | Event time and ledger write time. |
| `source`, `mode` | text | Normalized source and required paper/live mode. |
| `outcome_status` | text | Observed outcome, not a recommendation. |
| `main_reason`, `failed_check` | text nullable | Existing primary reason and first failing check, when known. |
| `strategy_version`, `agent_version` | text nullable | Explicitly supplied provenance only; never inferred. |
| `parameters_json` | jsonb | Bounded strategy/config identifiers and values used for review. |
| `risk_checks_json` | jsonb | Bounded denormalized summary for one-row history reads. |
| `provider_data_json` | jsonb | Bounded provider names, statuses, and field provenance only. |
| `snapshot_id` | UUID nullable | References `provider_snapshots`. |
| `related_trade_id`, `related_candidate_id` | text nullable | Existing source IDs, not assumed foreign keys until backfill coverage is proven. |
| `supersedes_decision_id` | UUID nullable | Corrects an imported or malformed diagnostic record without destructive updates. |

Suggested indexes: `(mint_address, decision_time DESC)`, `(outcome_status, decision_time DESC)`, `(source, mode, decision_time DESC)`, and `(related_trade_id)` where non-null.

### `memecoin_decision.risk_check_results`

One normalized result per evaluated check. Consumer: rejection reasoning and provider/data-gap aggregation.

| Field | Proposed type | Notes |
| --- | --- | --- |
| `risk_check_result_id` | UUID PK | Immutable row ID. |
| `decision_id` | UUID FK | Required parent decision. |
| `check_name` | text | Existing normalized check names such as `liquidity_check`. |
| `result` | text | Exactly `PASS`, `FAIL`, or `UNKNOWN`. |
| `reason_code` | text nullable | Explicit normalized reason only. |
| `observed_value_json`, `threshold_json` | jsonb | Allowlisted values used to explain a result. |
| `provider_snapshot_id` | UUID nullable | Evidence source, when applicable. |
| `checked_at` | timestamptz | Time of the check observation. |

Unique `(decision_id, check_name)` preserves the current one-result-per-check snapshot model. `UNKNOWN` is valid and must not be coerced to `FAIL` or excluded from reads.

### `memecoin_decision.rejection_records`

Details for a rejected or otherwise blocked decision. Consumer: recent-rejection and missed-winner review.

| Field | Proposed type | Notes |
| --- | --- | --- |
| `rejection_id` | UUID PK | Immutable rejection evidence row. |
| `decision_id` | UUID FK unique | One primary rejection record per decision. |
| `rejection_reason`, `failed_check` | text | Existing normalized diagnostic values. |
| `risk_profile`, `candidate_mode` | text | Strict/discovery profile and launch/migration/unknown context. |
| `attention_score`, `risk_score` | numeric nullable | Diagnostic values only. |
| `snapshot_coverage_status` | text | `covered`, `legacy_missing`, or explicit future states. |
| `created_at` | timestamptz | Ledger observation time. |

### `memecoin_decision.provider_snapshots`

Normalized, redacted evidence observation. Consumer: provider provenance and data-quality review.

| Field | Proposed type | Notes |
| --- | --- | --- |
| `snapshot_id` | UUID PK | Referenced by decisions and results. |
| `mint_address` | text | Required subject mint. |
| `provider_name`, `provider_status` | text | Provider identity and explicit result state. |
| `observed_at` | timestamptz | Provider observation time. |
| `field_presence_json` | jsonb | Present normalized field names, not raw payload. |
| `normalized_data_json` | jsonb | Bounded values needed by consumers only. |
| `unavailable_reason` | text nullable | Explicit non-success reason when known. |
| `source_decision_id` | UUID nullable | Optional decision context. |
| `content_hash` | text nullable | Optional dedupe of normalized evidence only. |

No raw HTTP request/response, token, RPC endpoint, transaction, or wallet secret is stored.

## Trade And Attribution Tables

These tables mirror existing facts; they do not become execution state.

### `trade_entries` And `trade_exits`

Both have `ledger_trade_id` UUID PK, `source_trade_id` text unique, `decision_id` UUID FK nullable, `mint_address`, `mode`, `executed_at`, `amount_sol`, `token_amount` nullable, `price_sol` nullable, `status`, `fill_quality` nullable, `evidence_json`, and `created_at`.

`trade_exits` additionally has `position_id` nullable, `exit_reason`, and `sell_pct` nullable. The exit writer records an already-persisted event and never invokes an adapter or transaction builder.

### `paper_positions`

Fields: `ledger_position_id` UUID PK, `source_position_id` text unique, `entry_trade_id`, `mint_address`, `mode` constrained to `paper` for this table, `status`, `opened_at`, `closed_at` nullable, `fill_quality`, `entry_price_sol`, `close_price_sol` nullable, `realized_pnl_sol` nullable, `pnl_confidence`, `evidence_json`, and `created_at`.

`realized_pnl_sol` is a persisted source observation, not a claim of attributable strategy performance.

### `attribution_links`

Fields: `attribution_link_id` UUID PK, `decision_id` UUID FK, `ledger_trade_id` UUID nullable, `ledger_position_id` UUID nullable, `link_type`, `verification_status`, `evidence_json`, `linked_at`, and `supersedes_attribution_link_id` nullable.

Only `verification_status = verified` permits a downstream PnL or performance view to call a decision attributable. Absent, pending, ambiguous, or rejected links keep attribution blocked.

## Rejection Outcome Tables

### `rejection_baselines`

Fields: `baseline_id` UUID PK, `rejection_id` UUID FK unique, `price_sol` nullable, `liquidity_sol` nullable, `provider_name`, `observed_at` nullable, `availability_status`, `missing_reason` nullable, `source_snapshot_id` UUID nullable, and `created_at`.

A baseline can explicitly be unavailable. It must not be fabricated from a later mark.

### `later_marks`

Fields: `later_mark_id` UUID PK, `baseline_id` UUID FK, `price_sol` nullable, `liquidity_usd` nullable, `provider_name`, `provider_status`, `observed_at`, `missing_reason` nullable, `source_snapshot_id` UUID nullable, and `created_at`.

Later-mark writes are bounded and read-only with respect to market providers. Missing marks remain explicit rows only if their status is useful to a consumer; otherwise the tool returns an unavailable result without writing.

### `outcome_labels`

Fields: `outcome_label_id` UUID PK, `rejection_id` UUID FK, `baseline_id` UUID FK, `later_mark_id` UUID FK, `label`, `return_multiple` numeric nullable, `label_version`, `evidence_status`, `labeled_at`, and `created_at`.

The writer refuses a non-`inconclusive` label unless linked baseline and later-mark rows contain valid positive prices. Labels are diagnostic review evidence, never ranking, filtering, sizing, acceptance, execution, PnL, or live-readiness inputs.

## Future-Only Learning Tables

Do not migrate or write these until outcome coverage and attribution are measured adequate in a later design task.

- `training_examples`: versioned immutable examples linked to verified evidence, with a `label_status` that preserves inconclusive evidence.
- `model_scores`: model/version predictions and score provenance, separate from runtime decision outputs.
- `parameter_reviews`: reviewed hypotheses, scope, sample criteria, evidence links, reviewer, and decision. It does not carry mutable strategy configuration.

None may train, deploy, or tune runtime behavior automatically.

## Narrow Tool Contracts

Tools validate allowlisted fields, return structured records, and expose no SQL, connection string, secret, or arbitrary table name.

| Tool | Inputs | Output | Boundary |
| --- | --- | --- | --- |
| `write_memecoin_decision` | one normalized decision plus optional normalized checks/snapshot references | IDs and write status | Records after an existing event; never evaluates or changes it. |
| `write_trade_entry` | existing source trade ID and redacted observed entry fields | ledger trade ID and status | Does not execute a swap or create a position. |
| `write_trade_exit` | existing source trade ID, linked entry/position IDs, and observed exit fields | ledger trade ID and status | Does not close a position or calculate PnL. |
| `write_rejection_outcome` | rejection ID plus observed baseline, later mark, or label evidence | affected IDs and evidence status | Rejects unsupported labels and missing linked evidence. |
| `read_coin_history` | mint address, bounded limit, optional time range | chronological decisions/evidence links | Read-only, evidence-first. |
| `read_recent_rejections` | bounded limit, source/mode/check filters | rejection summaries with unknown coverage | Read-only; no recommendations. |
| `read_missed_winners` | bounded time range and only diagnostic label criteria | measurable labeled rejections plus coverage counts | Requires linked valid baseline/later marks; inconclusive rows stay visible. |
| `read_trade_reasoning` | source trade or decision ID | linked decision, checks, evidence, and attribution status | Never claims PnL without a verified link. |
| `summarize_learning_progress` | bounded time range and version filters | coverage, labels, verification, and data-gap counts | No performance claim unless requested evidence is verified. |

## Required Invariants

- Every table and persisted field has a named downstream consumer.
- Unknown remains unknown across imports, reads, summaries, and labels.
- A decision does not become accepted because it appears in the ledger.
- Outcome labels require linked baseline and later-mark evidence; otherwise they are `inconclusive`.
- No tool emits a live-readiness, execution authorization, or transaction-submission claim.
- PnL or performance attribution requires a verified `attribution_links` row and the applicable trade/position evidence.
- All imports preserve source IDs and timestamps where available, label legacy missing evidence explicitly, and never backfill invented provider data.

## Implementation Phases

1. Add a reviewed schema migration and narrow tool definitions using no runtime callers. Test constraints, redaction, idempotency, and read-only role boundaries with synthetic fixtures.
2. Add a backfill-safe diagnostic importer for selected existing `paper_decisions`, trades, and positions. Preserve IDs; report covered versus legacy-missing evidence; do not write to SQLite.
3. Enable explicit diagnostic-only tool writes from an operator or import workflow, with bounded verification that ledger failure cannot affect SQLite persistence.
4. Add read tools for coin history, recent rejections, outcome coverage, and trade reasoning. Verify every PnL/performance response enforces attribution links.
5. Consider runtime integration only after separate approval, migration verification, independent coverage review, and proof that non-blocking failure behavior preserves existing safety gates.
