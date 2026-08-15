# Pre-Built Exit Transactions (Design Only — MT-560)

## Purpose

Design for eliminating the Jupiter quote round-trip on the SELL side of a
live Strategy B position. Today, every exit (take-profit, trailing stop, hard
stop, time stop) starts with a fresh Jupiter quote request before the swap is
built, signed, and sent. That quote call costs roughly 300ms of exit latency
— time during which the exit price can move.

This document records the approach only. **Nothing here is implemented.**
This is a live-trading optimization and must not ship without explicit
approval and a micro-live review.

## Current Exit Path (Live Mode)

On a close signal, `monitor_positions` calls `_adapter_close`, which calls
`LiveExecutionAdapter.sell(mint, token_amount)`:

1. `_check_token_allowed` — banned-mint set check (in-memory).
2. `_check_sol_balance` — SOL balance check (RPC call).
3. `JupiterSwapClient.get_quote(...)` — Jupiter Swap API quote (HTTP round
   trip, ~200-400ms).
4. `_check_price_impact` — gate on the returned quote (in-memory).
5. `_request_swap_transaction(...)` — Jupiter `/swap/v1/swap` request
   (HTTP round trip).
6. Sign with the wallet keypair (local, sub-ms).
7. `_send_transaction(...)` — RPC `sendTransaction` (HTTP round trip).
8. `_confirm_signature(...)` — poll `getSignatureStatuses` until confirmed
   (up to 30s).

The quote is a blocking dependency: nothing is signed or sent until the quote
returns. Exit latency from signal to send ≈ quote + swap-tx + send + confirm.

## Proposed Design: Cache the Exit Template at Entry

The core idea: at buy confirmation time, capture everything about the exit
transaction that does NOT change between entry and exit, then on exit only
refresh the volatile pieces.

### At Buy Confirmation (entry time)

1. Record the token's mint and the wallet's token account
   (`getTokenAccountsByOwner` result — the token account ATA is created by the
   swap, so capture it right after the buy confirms).
2. Capture the pool/program context from the Jupiter buy route
   (`route_plan` — which DEX program and pool the token trades on). This
   identifies the likely sell route without needing a fresh quote.
3. Cache the token decimals (already available via
   `getTokenSupply`/decimals cache in `JupiterSwapClient`).
4. Store these in memory keyed by mint (and, for crash recovery, in the
   position's `metadata_json`).

### At Exit Trigger (exit time)

1. Look up the cached exit template for the mint.
2. Refresh only the volatile inputs:
   - Current token balance (the buy amount minus any partial exits).
   - Fresh blockhash + last valid block height (one RPC call, already part of
     the swap build path).
   - Current pool reserves (one extra RPC call if the route needs exact
     reserve amounts — most Jupiter v6+ routes quote via their own API, so
     this may be unnecessary for the swap-tx request path).
3. Plug the cached route/program/account info into the swap-tx request —
   or, where the route is still quote-based, use the cached account context to
   short-circuit the price-impact pre-check rather than skipping the quote
   entirely.

### Implementation Boundaries

- **Do not hard-code a route.** Jupiter routes change (pool rotation,
  migrates). The cache is a *hint* that must be validated at exit; if the
  cached route fails, fall back to the current fresh-quote path. The fallback
  is mandatory — a pre-built exit that silently breaks is worse than no cache.
- **Cache freshness:** invalidate the cache for a mint after N minutes or
  after any failed exit attempt, so stale pool accounts never poison exits.
- **Partial exits:** after any sell, update the cached token balance from the
  fill; never hard-code the original buy amount.
- **Memory bounds:** the cache is per-open-position (≤ `MAX_OPEN` = 5
  positions), so no eviction policy beyond closing the position is needed.
- **Safety gates unchanged:** the 5% price-impact gate and the circuit
  breaker remain fully enforced. The pre-built path only changes *how* the
  swap transaction is constructed, never the gating around it.

### Expected Benefit

Removes one Jupiter quote round trip (~300ms) from the exit critical path.
With the quote gone, exit latency to send becomes: blockhash refresh + swap-tx
request + send ≈ 400-600ms instead of 700-1000ms.

### Risk / Watch Items

- Exits are the loss-limiting path — a cache bug that delays or breaks a sell
  is the worst failure mode this system can have. The fresh-quote fallback
  must be tested under simulated stale-cache conditions before live use.
- Confirmation polling dominates exit latency when the network is congested;
  the pre-built path does not reduce confirm time, only time-to-send.
- The circuit breaker already trips on failed sells; a broken pre-built exit
  would surface there, which is the designed containment.

## Status

Design only. No code changes. Do not implement as part of MT-560.
