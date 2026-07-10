# Helius Readiness Provider Wiring Design

## Current State

Three protocol-based providers are defined but have no real implementations,
keeping `live-readiness` at `NOT READY` for all three:

| Provider | Protocol | File | Current Diagnostic |
|---|---|---|---|
| `wallet_balance_lookup` | `SupportsWalletBalanceLookup` | `live_preflight.py:11` | `wallet_balance_lookup_unavailable` |
| `transaction_simulator` | `SupportsTransactionSimulation` | `live_preflight.py:15` | `transaction_simulator_unavailable` |
| `wallet_holdings_lookup` | `SupportsWalletHoldingsLookup` | `position_reconciliation.py:12` | `wallet_holdings_lookup_unavailable` |

### Protocol Signatures

```
SupportsWalletBalanceLookup     async () -> float | None
SupportsTransactionSimulation   async (transaction: str | bytes) -> object
SupportsWalletHoldingsLookup    async () -> dict[str, float] | None
```

All three return `None` or raise an exception to signal unavailability/failure.

## Required Env/Config

| Var | Needed For | Readiness Only |
|---|---|---|
| `HELIUS_API_KEY` | All three providers (RPC calls) | Yes — transaction_simulator only |
| `HELIUS_RPC_URL` | All three (calculated from API key if not set) | Yes |
| `PRIMARY_RPC_URL` | Optional RPC override | Yes |
| `TRADING_WALLET_PUBLIC_KEY` | wallet_balance_lookup, wallet_holdings_lookup | Yes — read-only |
| `TRADING_WALLET_PRIVATE_KEY` | Actual signing for live buy/exit | No — only for live execution, not readiness |

Key distinction: `TRADING_WALLET_PUBLIC_KEY` is sufficient for read-only readiness
checks (balance, holdings). The private key is only loaded at live-swap time.

## Proposed Classes

### 1. `src/execution/helius_providers.py` (new file)

Three small adapter classes, each conforming to its protocol:

```python
class HeliusBalanceLookup:
    """SupportsWalletBalanceLookup via Helius RPC getBalance."""
    def __init__(self, rpc_url: str, wallet_address: str) -> None: ...
    async def __call__(self) -> float | None: ...

class HeliusTransactionSimulator:
    """SupportsTransactionSimulation via Helius RPC simulateTransaction."""
    def __init__(self, rpc_url: str) -> None: ...
    async def __call__(self, transaction: str | bytes) -> TransactionSimulationResult: ...

class HeliusWalletHoldingsLookup:
    """SupportsWalletHoldingsLookup via Helius RPC getTokenAccountsByOwner."""
    def __init__(self, rpc_url: str, wallet_address: str) -> None: ...
    async def __call__(self) -> dict[str, float] | None: ...
```

### 2. Optional factory function

```python
def build_helius_providers(settings: Settings) -> ProvidersBundle:
    """Construct providers if HELIUS_API_KEY is available, else return None entries."""
```

Returns a dataclass with optional fields so the readiness gate can distinguish
"config missing" from "providers unhealthy".

## Files to Touch

- `src/execution/helius_providers.py` — **NEW**, the three adapter classes + factory
- `src/execution/live_readiness.py` — wire providers into evaluation (already accepts optional params)
- `src/execution/live_preflight.py` — already calls the protocols, no change needed
- `src/execution/position_reconciliation.py` — already calls the protocol, no change needed
- `src/cli.py` — update `live-readiness` command to pass built providers
- `src/core/config.py` — add `TRADING_WALLET_PUBLIC_KEY` to settings if needed (currently only `TRADING_WALLET_PRIVATE_KEY` in `.env.example`)
- `tests/test_helius_providers.py` — **NEW**, fake-HTTP-client tests for all three providers

## Safety Constraints

1. **No secret leakage**: RPC URLs contain the API key as a query parameter.
   Provider logs must redact or omit the URL. The `evaluate_live_execution_config`
   already redacts RPC URLs down to host labels — reuse that pattern.

2. **No raw transaction leakage**: `transaction_simulator` receives raw tx bytes
   but must never log them. Return only `TransactionSimulationResult(ok, error)`.

3. **Fail closed**: Any exception, timeout, malformed response, or missing config
   returns `None` (for balance/holdings lookups) or raises for `transaction_simulator`,
   which the readiness gate maps to `*_unavailable` or `*_failed`.

4. **No live trading**: Providers are read-only — they query state, never submit.
   They should never be able to sign or send transactions.

5. **Default mode paper**: Providers only affect `live-readiness` diagnostics.
   Paper mode does not use them.

6. **No .env committed**: Values always read from runtime env or `.env` file,
   never hardcoded.

## Fake-Client Test Strategy

All three providers accept an injectable HTTP client (following the pattern in
`src/chain/rpc.py`'s `SolanaRpcClient` or `src/chain/jito.py`):

```python
class HeliusBalanceLookup:
    def __init__(self, rpc_url: str, wallet_address: str,
                 http_client: httpx.AsyncClient | None = None) -> None:
        self._client = http_client or httpx.AsyncClient()
```

Tests inject a fake HTTP client (or mock `httpx.AsyncClient.post`) to simulate:

| Scenario | HTTP Response | Expected Provider Output |
|---|---|---|
| Healthy balance | `{"result": {"value": 1.5}}` | `1.5` |
| Empty wallet | `{"result": {"value": 0.0}}` | `0.0` |
| RPC error | HTTP non-200 | `None` (or raises) |
| Timeout | `httpx.TimeoutException` | `None` (or raises) |
| Malformed JSON | garbage body | `None` (or raises) |
| Missing API key (no client) | N/A — no RPC URL to build | `None` from factory |

This ensures zero real network calls in normal tests.

## How Failures Close to NOT READY

The existing flow in `evaluate_live_preflight` and `reconcile_positions` already
handles this:

- Provider returns `None` → `wallet_balance_unknown` / `wallet_holdings_unknown`
- Provider raises → `wallet_balance_lookup_failed` / `transaction_simulation_failed_provider`
- No provider passed → `wallet_balance_lookup_unavailable` / `transaction_simulator_unavailable`

The new providers would naturally trigger these paths when RPC is down or config
is missing, without any additional logic.

## Implementation Order

1. **transaction_simulator first** (no wallet key needed, only `HELIUS_API_KEY`)
2. **wallet_balance_lookup** (needs `TRADING_WALLET_PUBLIC_KEY` — add to settings)
3. **wallet_holdings_lookup** (needs `TRADING_WALLET_PUBLIC_KEY` — same setting)

Each step is independently testable and independently wireable into readiness.
Wiring a single provider does not make `live-readiness` `READY` — all guardrails,
config, and other checks still apply.

## Readiness Trace (After Full Wiring)

```
env_readiness_ready=NO
  HELIUS_API_KEY=present
  TRADING_WALLET_PUBLIC_KEY=present
  TRADING_WALLET_PRIVATE_KEY=MISSING
  ...

live-readiness:
  guardrails=not_ready                    # still in paper mode
  execution_config=ok
  preflight=ok                            # both providers healthy
  position_reconciliation=ok              # holdings lookup healthy
  circuit_breaker=ok
  health=ok
micro_live_ready=NOT READY               # guardrails not satisfied
```

Live trading is still blocked by guardrails even when all providers are healthy.
