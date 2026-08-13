# Jupiter Live Trading Research: Money, Technology, and Risk

**Task:** MT-532  
**Research date:** 2026-08-13 UTC  
**Scope:** Strategy B, 0.05 SOL paper entries, Solana/Jupiter execution  
**Safety:** Research only. No code, wallet, database, or running process was changed.

## Executive Summary

Strategy B is not ready for an unguarded live launch. The paper edge is positive, but it is fragile to execution friction:

- The live database snapshot contained **638 closed Strategy B positions** and **+3.310840 SOL** realized paper PnL.
- A symmetric slippage stress test on every entry and exit remains positive at **1% (+2.634732 SOL)** and **2.5% (+1.620569 SOL)**, but turns slightly negative at **5% (-0.069702 SOL)**. At 10% it is **-3.450244 SOL**.
- The requested sample of the 20 newest trades contains only **4/20 positive stored entry-liquidity values**. The other 16 records have `liquidity: {}` in the persisted DexScreener metadata. DexScreener's public API provides current pair data, not a historical point-in-time replay endpoint, so current liquidity must not be presented as entry liquidity.
- A 0.05 SOL swap is large relative to a $2K-$50K token's likely pool depth. For the lowest-liquidity end of the range, **5-10% realized execution friction is plausible**, not an acceptable fixed assumption. Jupiter's quote must be the source of truth before signing.
- Solana's base fee is negligible: 5,000 lamports per signature, or 0.000005 SOL. Priority fees can be much larger during competition. Jupiter's current V2 documentation recommends dynamic/local fee estimation, rather than a hard-coded universal lamport number.
- Recommended first hot-wallet funding is **1.0 SOL**, with **0.5 SOL** as the absolute test floor. This assumes four simultaneous 0.05 SOL positions, a 0.20 SOL exposure cap, multiple exit retries, account/rent overhead, fee/tip buffer, and a reserve that is not traded.
- The existing system's live adapter is intentionally closed by default and currently has `execute_swap`, `get_quote`, and `get_current_price` as phase-gated placeholders. This report does not change that boundary.

## 1. The Money

### 1.1 Data set and method

The analysis used a read-only query against `data/trades.db` while Strategy B was active. Counts can grow after the query time.

Relevant persisted facts:

| Metric | Value |
|---|---:|
| Closed Strategy B positions | 638 |
| Paper entry size | 0.05 SOL |
| Total paper entry notional | 32.15 SOL |
| Paper realized PnL | +3.310840 SOL |
| Average paper PnL per closed trade | +0.005189 SOL |
| Strategy B candidate entries | 638 |
| Entry rows with positive stored liquidity | 213/638 (33.4%) |
| Positive stored-liquidity average | $14,864.09 |
| Positive stored-liquidity range | $27.44-$91,638.89 |

Paper PnL is not a fill-quality measurement. The runtime records DexScreener-derived entry price and uses a paper quote provider; it does not model the actual route, pool curve, priority fee, token-account setup, or failed execution.

### 1.2 Requested 20-trade sample

The sample is the 20 most recent closed Strategy B positions at query time, ordered by `opened_at` descending. `Stored entry liquidity` means a positive value persisted in the entry candidate/metadata. A blank means the runtime stored an empty liquidity object or no positive numeric value. It is **not** a claim that the pool had zero liquidity.

| # | Ticker | Mint prefix | MCap at entry | Stored entry liquidity | Paper PnL |
|---:|---|---|---:|---:|---:|
| 1 | BOIÚNA | `2sxawpgXrc...` | $2,910 | unavailable | -0.005000 |
| 2 | Qenis | `GHy2B5dSfy...` | $18,388 | unavailable | -0.005000 |
| 3 | Qenis | `GtVX9dtHVx...` | $18,499 | unavailable | -0.005000 |
| 4 | BOIÚNA | `BwPmJRtzTK...` | $4,007 | unavailable | -0.003197 |
| 5 | ZAZU | `63a5y25Wr8...` | $2,142 | unavailable | 0.000000 |
| 6 | BOIÚNA | `G1KzPR2HZA...` | $2,679 | unavailable | 0.000000 |
| 7 | BOIÚNA | `7jT1vwbB4e...` | $2,122 | unavailable | 0.000000 |
| 8 | Qenis | `EkcTa8n14f...` | $6,928 | unavailable | +0.040000 |
| 9 | ANYKEY | `A8hVkt51KD...` | $32,745 | $5,579.04 | -0.005000 |
| 10 | susdog | `3xZNkyngcU...` | $32,764 | $8,381.53 | +0.040000 |
| 11 | XST | `ARSzAJZRfi...` | $33,124 | unavailable | +0.040000 |
| 12 | CHAM | `9cZpc6Nfbk...` | $40,614 | unavailable | +0.040000 |
| 13 | susdog | `HSmhKQPBE4...` | $31,623 | $9,275.96 | +0.040000 |
| 14 | XST | `HEmEdyUAaL...` | $7,743 | unavailable | -0.005000 |
| 15 | CHAM | `32kWnr3mer...` | $36,778 | unavailable | -0.005000 |
| 16 | XST | `965mefZFtX...` | $10,327 | unavailable | 0.000000 |
| 17 | XST | `965mefZFtX...` | $10,327 | unavailable | 0.000000 |
| 18 | susdog | `HVAYF7YvNj...` | $3,812 | $3,194.21 | -0.004159 |
| 19 | PITCOIN | `CWCzbbvsjB...` | $34,163 | unavailable | +0.003873 |
| 20 | MOONDENG | `9dYvvZ5789...` | $36,572 | unavailable | -0.005000 |

**Sample result:** 4/20 have positive stored liquidity; 16/20 are unavailable. The sample spans approximately $2.1K-$40.6K market cap and includes repeated ticker names that are different mints. This is exactly the market where market-cap alone is a poor execution-quality proxy.

### 1.3 Why current DexScreener data cannot answer historical liquidity

The DexScreener API documents current endpoints such as:

- `GET /tokens/v1/{chainId}/{tokenAddresses}`
- `GET /token-pairs/v1/{chainId}/{tokenAddress}`
- `GET /latest/dex/pairs/{chainId}/{pairId}`

These return the current pair snapshot. The public API does not expose a time-series query that reconstructs liquidity at an arbitrary historical timestamp. A current request was performed for the 20 sampled mints as a sanity check, but it is not used as entry liquidity because the trades are from earlier timestamps and several pools have since migrated, drained, or changed pair.

The stored record itself shows the root cause. For example, the newest entries persist:

```json
"liquidity": {}
```

while some entries persist `{"usd": 5579.04, ...}`. The correct follow-up measurement is to record the Jupiter route's `priceImpactPct`, `outAmount`, route plan, pool addresses, and actual fill amounts at live-entry time. A historical research collector would need to capture DexScreener or on-chain pool state continuously; it cannot be reconstructed reliably from today's API response.

### 1.4 Realistic slippage for 0.05 SOL

There are two different quantities that are often called slippage:

1. **Jupiter slippage tolerance:** the maximum adverse movement allowed before the transaction fails (`slippageBps`).
2. **Realized execution friction:** route price impact plus movement between quote, signing, and landing, plus any MEV effect. This is what changes PnL.

For a 0.05 SOL order in a token with $2K-$50K market cap:

| Market condition | Working realized-friction assumption | Interpretation |
|---|---:|---|
| Deep enough route, low activity | 1-2% round trip | Possible, but must be observed from Jupiter quotes/fills |
| Typical fresh meme route | 2.5-5% round trip | Reasonable stress range for initial paper-to-live comparison |
| Thin or rapidly moving pool | 5-10%+ round trip | Plausible; do not trade if quote impact is already high |
| Pool drained, migration, honeypot, or failed exit | 100% loss of position | Slippage setting cannot solve this |

The trade size is especially material near $2K-$5K market cap. Market cap is not pool liquidity, and a bonding curve or a pool with only a few thousand dollars of usable two-sided depth can move dramatically from a 0.05 SOL buy or sell. The execution gate should reject based on the actual Jupiter quote and route impact, not on a fixed market-cap threshold.

Suggested initial live policy for a future guarded test:

- Quote both directions when possible: SOL -> token before entry and token -> SOL immediately after entry simulation.
- Reject if Jupiter reports route `priceImpactPct` above **2%** for the full order, or if the expected round-trip impact exceeds **4%**.
- Start with fixed tolerance around **300 bps (3%)** only as a transaction-failure ceiling, not as an expected-cost assumption.
- Do not increase tolerance to force a trade through a thin pool. A successful fill at a bad price is worse than a failed transaction.
- Record quote time, context slot, route plan, `inAmount`, `outAmount`, `otherAmountThreshold`, `priceImpactPct`, actual output, and fee fields for every fill.

### 1.5 PnL under slippage

The stress test applies a symmetric percentage haircut to both the buy and sell legs of every closed paper trade. For a trade with paper entry `E` and paper PnL `P`, the stressed result is:

```text
stressed_pnl = (E + P) * (1 - s) - E * (1 + s)
```

This is intentionally conservative and simple. It treats the paper entry as too expensive by `s` and the paper exit as too cheap by `s`; it does not model partial exits, price path, or liquidity-dependent impact.

| Symmetric friction on each leg | Strategy B PnL over 638 trades | Avg per trade |
|---:|---:|---:|
| 0% paper baseline | +3.310840 SOL | +0.005189 SOL |
| 1% | +2.634732 SOL | +0.004129 SOL |
| 2.5% | +1.620569 SOL | +0.002540 SOL |
| 5% | -0.069702 SOL | -0.000109 SOL |
| 10% | -3.450244 SOL | -0.005408 SOL |
| 20% | -10.211328 SOL | -0.016004 SOL |

The break-even symmetric friction is approximately **4.9% per leg** under this model. That is an important warning: +3.31 SOL paper PnL does not leave room for routinely poor fills.

This is not proof that live PnL will be exactly one of these rows. It is a sensitivity analysis. The 638 paper trades were marked with DexScreener prices, and the paper exit logic can close at a modeled trigger price. Live execution will add route impact, quote staleness, priority fees, token-account setup/rent behavior, and failed/partial operational outcomes.

### 1.6 Base and priority fees

Solana's base transaction fee is **5,000 lamports per signature = 0.000005 SOL**. A buy and a sell therefore cost at least approximately **0.000010 SOL** in base fees, assuming one wallet signature each. This is only 0.02% of a 0.05 SOL entry and is not the main economic concern.

Priority fee is:

```text
ceil(compute_unit_price_micro_lamports * compute_unit_limit / 1,000,000)
```

Jupiter's V1/Metis documentation recommends `prioritizationFeeLamports` with a local fee market and a cap, and `dynamicComputeUnitLimit: true`. Its current V2 documentation recommends local priority-fee percentile selection and notes that `mode=fast` uses a higher default percentile. The correct fee is time- and writable-account-dependent; there is no honest fixed amount that guarantees sub-second landing.

As a scale example, at a 300,000 CU limit:

| CU price | Priority fee | SOL | Comment |
|---:|---:|---:|---|
| 10,000 micro-lamports/CU | 3,000 lamports | 0.000003 | Quiet network example |
| 100,000 micro-lamports/CU | 30,000 lamports | 0.000030 | Moderate competition example |
| 500,000 micro-lamports/CU | 150,000 lamports | 0.000150 | Active-period example |
| 1,000,000 micro-lamports/CU | 300,000 lamports | 0.000300 | Expensive active-period example |

The current public Solana `getRecentPrioritizationFees` request returned zero for the sampled slots, which is a point-in-time observation, not a guarantee for an active meme period. For a first live implementation, use Jupiter's local estimator with a hard maximum, collect the returned fee and landing latency, and tune from observed p50/p90 data. A practical initial operational ceiling is **0.001 SOL per transaction** for priority fee plus tip, with a circuit breaker when the estimator exceeds it. This is a guardrail, not a prediction.

Jupiter's newer landing services have different economics:

- `/swap/v2/order` + `/execute` gives managed landing and may apply a Jupiter swap fee depending on route/pair. It is not the same as a zero-fee raw router transaction.
- `/swap/v2/build` has no Jupiter swap fee, but self-managed submission needs an RPC or Jupiter's `tx.jup.ag` submission path.
- `tx.jup.ag` requires a Jupiter tip of at least **0.001 SOL** and is send-only; confirmation must happen through the project's own RPC. This minimum is much larger than the Solana base fee and should not be used for a tiny first smoke trade unless the landing benefit is demonstrated.

### 1.7 Recommended starting capital

Minimum position exposure with four positions is:

```text
4 positions * 0.05 SOL = 0.20 SOL
```

That is not sufficient wallet funding because the wallet also needs SOL for priority fees, retry attempts, associated-token-account/rent behavior, and the possibility of an exit while all four positions are open.

| Funding level | Recommendation |
|---:|---|
| 0.20 SOL | Not enough: only the four entry notionals, no operational reserve |
| 0.50 SOL | Absolute floor for a tightly capped test; stop if reserve drops below 0.20 SOL |
| **1.00 SOL** | Recommended first funded hot wallet; 0.20 SOL maximum open exposure and ~0.80 SOL retained reserve |
| 2.00 SOL+ | Not justified until fills, landing, and reconciliation are measured |

The recommended 1.0 SOL is a risk-control recommendation, not a claim that 1 SOL can absorb a rug. The wallet should be dedicated, have no unrelated assets, use a separate signer or narrowly funded hot wallet, and be replenished only after reviewing trade reconciliation. The strategy's maximum daily loss should be materially below the wallet balance; a starting cap of **0.10 SOL daily loss** and **0.20 SOL daily notional exposure** is more appropriate than allowing the wallet to be fully deployed.

## 2. The Technology

### 2.1 Requested Jupiter V6 flow

The historical Jupiter V6/Metis flow is:

```text
GET /quote
  -> POST /swap
  -> decode base64 swapTransaction
  -> sign VersionedTransaction
  -> sendRawTransaction through Solana RPC
  -> confirm with signature + blockhash/lastValidBlockHeight
```

Typical V6 endpoints were `https://quote-api.jup.ag/v6/quote` and `https://quote-api.jup.ag/v6/swap`. The current official documentation marks Metis `/swap/v1` as no longer actively maintained and recommends Swap API V2. Treat V6 knowledge as useful for understanding the existing task/request, but do not start a new production integration against an undocumented legacy host.

### 2.2 Current recommended Jupiter choices

Jupiter's current API has two paths:

**Meta-Aggregator:**

```text
GET https://api.jup.ag/swap/v2/order
  -> sign returned transaction
  -> POST https://api.jup.ag/swap/v2/execute
```

This path lets multiple routing engines compete and gives Jupiter responsibility for landing, retries, and confirmation. It is the simpler choice when the application does not need to modify the transaction.

**Router/custom transaction:**

```text
GET https://api.jup.ag/swap/v2/build
  -> simulate/build v0 transaction with returned instructions
  -> sign
  -> send to own RPC or tx.jup.ag
  -> confirm through own RPC
```

This path provides transaction control and no Jupiter swap fee, but the application owns transaction assembly, compute-limit simulation, submission, confirmation, and retry behavior. It is the better fit if the project needs Jito-specific instructions, custom assertions, or a controlled MEV path.

### 2.3 Parameters that matter

#### `slippageBps`

One basis point is 0.01%. For an exact-in swap, it sets the minimum output accepted by the transaction. It is a failure boundary, not a guaranteed realized loss. On a rapidly moving meme token, a very low value causes failures; a high value permits a fill at a harmful price. Start with a bounded value and reject based on quote impact before signing.

#### `prioritizationFeeLamports`

In V6/Metis this can request a fixed amount or a percentile/maximum fee estimate. Prefer the local fee market (`global: false`) and a hard cap. The API schema notes that this field selects either a priority fee or a Jito tip, not both; to include both, use instruction-level construction.

#### `dynamicComputeUnitLimit`

When enabled, Jupiter simulates the swap and sets a more accurate compute limit. This usually lowers overpayment compared with a generic maximum and improves landing. In a custom V2 `/build` flow, simulate with a high limit, then use approximately 1.2x observed units, capped at Solana's 1.4M transaction CU maximum.

#### Other controls worth recording

- `restrictIntermediateTokens=true` reduces exposure to arbitrary intermediate routes.
- `maxAccounts` affects route availability and transaction size. Jupiter recommends keeping it as high as possible unless size is a problem.
- `asLegacyTransaction` is only for wallets or routes that cannot use v0 versioned transactions; it can reduce route availability.
- `wrapAndUnwrapSol` controls SOL/WSOL setup and cleanup.
- `blockhashSlotsToExpiry` can shorten validity so a stale meme trade fails quickly instead of landing much later.
- `priceImpactPct`, route plan, pool/AMM keys, and context slot must be persisted for post-trade analysis.

### 2.4 Python implementation approach

For the project's Python stack, use a narrow adapter with injected HTTP and RPC interfaces:

1. `httpx.AsyncClient` calls Jupiter's REST endpoint with timeouts, API-key headers when required, status checks, and redacted error logging.
2. Convert SOL to lamports and tokens to atomic units using mint decimals. Never pass floating point amounts to the API.
3. Parse the base64 transaction with `solders.transaction.VersionedTransaction.from_bytes`.
4. Sign the versioned message with `solders.keypair.Keypair` and `solders.message.to_bytes_versioned` or the supported transaction signing helper.
5. Serialize bytes and send through `solana-py` `AsyncClient.send_raw_transaction`, a dedicated RPC transport, Jito, or Jupiter's current transaction landing endpoint.
6. Confirm using the signature and the returned blockhash/last-valid height. Persist the signature even when confirmation later fails.

Minimal V6-shaped pseudocode:

```python
quote = await http.get(
    "https://api.jup.ag/swap/v1/quote",
    params={
        "inputMint": WSOL,
        "outputMint": token_mint,
        "amount": str(lamports),
        "slippageBps": 300,
        "restrictIntermediateTokens": "true",
    },
)
quote.raise_for_status()

swap = await http.post(
    "https://api.jup.ag/swap/v1/swap",
    json={
        "quoteResponse": quote.json(),
        "userPublicKey": wallet.pubkey(),
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": {
            "priorityLevelWithMaxLamports": {
                "priorityLevel": "veryHigh",
                "maxLamports": 1_000_000,
                "global": False,
            }
        },
    },
)
swap.raise_for_status()
unsigned = VersionedTransaction.from_bytes(base64.b64decode(swap.json()["swapTransaction"]))
signed = sign_versioned_transaction(unsigned, keypair)
signature = await rpc.send_raw_transaction(bytes(signed), opts=TxOpts(skip_preflight=False))
```

For a new implementation, replace the V6 endpoints with V2 `/order` + `/execute` or V2 `/build` + own submission. The existing project's `httpx`, `solders`, and `solana-py` dependencies are sufficient; no SDK is required.

### 2.5 Confirmation and retry logic

`sendTransaction` returning a signature only means an RPC accepted the request. It does not mean the transaction landed. Confirmation should:

1. Record the signature, quote, blockhash, and last-valid block height.
2. Poll `getSignatureStatuses` or `getTransaction` at `confirmed` for a short fast window, then `finalized` for accounting.
3. Treat `err != null` as a failed swap, not a position fill.
4. If status is absent, poll until a deadline based on the blockhash expiry. A signature can still be processed after a send call returns.
5. If the blockhash expires with no status, rebuild a fresh quote and transaction. Do not blindly resend an old signed transaction with an expired blockhash.
6. Before retrying a buy or sell, query the wallet token balance and recent transaction status. The first attempt may have landed even if the client timed out.
7. Use bounded retries, for example: immediate status check, one fresh-quote retry after 250-500 ms, one backup-RPC/Jito submission, then mark `execution_unknown` and alert. Never loop indefinitely.
8. For a sell, keep the position open until the wallet balance and transaction outcome reconcile. If the token balance changed, do not submit a duplicate full-size sell.

For Jito bundles, a returned bundle ID only means the block engine received the bundle. Poll `getBundleStatuses` or the in-flight status endpoint. Bundles are atomic and up to five transactions, but a low tip may lose the auction. Do not count a bundle as landed solely from the submission response.

### 2.6 Existing Python wrappers and libraries

- **`jupiter-python-sdk`** (`0xTaoDev/jupiter-python-sdk`, PyPI 0.0.2.0): wraps quote/swap and other Jupiter features using `solana-py`, `solders`, and `anchorpy`. It is old/experimental and its examples point at V6-era endpoints. Use as a reference, not a blind production dependency.
- **`jito-py-rpc`** (`jito-labs/jito-py-rpc`): Python JSON-RPC SDK for Jito transactions and bundles, including bundle-status methods.
- **Direct `httpx` + `solders` + `solana-py`:** preferred for this project because it keeps the execution boundary explicit, testable, and compatible with the project's existing injected fake transports.

The official Jupiter examples are primarily TypeScript, but the API is JSON/HTTP and does not require a JavaScript runtime. The wrapper's maintenance status, endpoint version, and signing behavior must be verified before use.

## 3. The Risk

### 3.1 Rug pull during the hold

A meme token can lose most or all value between two 30-second paper marks. A live monitor should use websocket account/log subscriptions where practical, with an RPC/DexScreener polling fallback. Polling every 5 seconds is a reasonable emergency-monitor target; 30 seconds is too slow for a fresh $2K pool.

Detection signals:

- Jupiter no longer returns a route or returns a sharply deteriorated quote.
- `priceImpactPct` or round-trip quote impact jumps above the emergency threshold.
- Pool liquidity falls sharply or the pool's quote-side reserve drains.
- Mint/freeze authority or Token-2022 transfer-hook risk changes.
- Wallet token balance, token-account state, or recent swap logs differ from the local position ledger.

The exit path should attempt a sell immediately, but it must also accept that a true drain can make the token unsellable. A rug is not bounded by the configured 10% hard stop; it can be a 100% position loss plus fees.

### 3.2 Failed sell transaction

The dangerous operational state is not simply `sell failed`; it is `local state says closed` while the wallet still holds tokens. The correct state machine is:

```text
exit requested -> transaction submitted -> outcome unknown
  -> confirmed success: reconcile token balance, then close
  -> confirmed error: fresh quote and bounded retry
  -> blockhash expired: rebuild and retry
  -> deadline exceeded: keep position open, alert, circuit-break new entries
```

Sell retries should use a fresh quote and should reduce size only when the wallet balance confirms a partial fill. A backup RPC and, where appropriate, Jito send path improve propagation but do not fix a bad route, a honeypot, or a depleted pool.

### 3.3 MEV and sandwich attacks

Solana's high throughput and public transaction flow make ordering and sandwich risk relevant, especially for low-liquidity meme pools. There is no useful universal prevalence percentage for this exact Strategy B universe; it must be measured from route quotes, pre/post execution price, and transaction ordering.

Mitigations:

- Keep slippage tolerance low enough to reject harmful fills, while using a quote-impact gate before signing.
- Submit through a protected/direct landing path rather than leaking raw transactions through many untrusted intermediaries.
- Use Jito or Jupiter landing infrastructure only after understanding its fee and confirmation semantics.
- Jito supports bundles, atomic execution, and a `jitodontfront` account mechanism intended to reduce sandwich ordering. Jito explicitly states that this is not a guarantee against every ordering path.
- For V2 `/order` + `/execute`, Jupiter's managed landing and RTSE are simpler than hand-assembling a raw transaction, but route/fee behavior must be recorded.
- Do not add a large slippage value simply to increase success rate. That converts failed trades into successful bad trades.

### 3.4 Wallet drainers and malicious token programs

An ordinary SPL token transfer does not give a token mint arbitrary authority over the wallet. The risk is the transaction and program interaction: a malicious or compromised program, Token-2022 transfer hooks, deceptive approval/signature flows, or a transaction builder that includes unexpected writable accounts or SOL transfers.

Controls:

- Decode and inspect the transaction before signing: program IDs, writable accounts, token mints, SOL transfers, setup/cleanup instructions, and any tip/referral account.
- Allowlist expected program IDs and reject unknown executable programs for the first live phase.
- Use a dedicated hot wallet containing only the test capital; never use a primary savings wallet.
- Never sign arbitrary browser prompts or opaque transactions from a token website.
- Simulate the exact serialized transaction and inspect simulation logs before submission.
- Treat Token-2022 transfer hooks and unknown extensions as a separate high-risk path; do not assume all SPL-compatible mints behave like classic SPL tokens.
- Keep private keys outside source control and outside logs. A Jupiter API key is not a wallet key, but it should still be scoped and rotated.

### 3.5 DexScreener price versus Jupiter route price

DexScreener is an aggregated display/indexing layer. Its pair price may be stale, may represent a different pair, and may be based on a different base/quote orientation. Jupiter's quote is route-specific and incorporates the executable pool path, fees, output amount, and price impact at quote time.

For this project, a live entry must use Jupiter's exact-input output amount as the fill estimate. DexScreener remains useful for discovery, age, market-cap context, and monitoring, but it is not an execution quote. The existing project status already identifies this separation: DexScreener is a price provider for paper workflows, while Jupiter live quoting is a separate phase-gated boundary.

Measure the difference explicitly after implementation:

```text
dex_price = DexScreener requested-mint/wSOL price at t0
jup_mid = Jupiter quoted output / input at t1
route_difference = (jup_mid / dex_price) - 1
```

Store both timestamps and pair/route identifiers. Compare by mcap and liquidity bucket. Do not use a current DexScreener response to backfill a historical Jupiter execution price.

### 3.6 Worst-case loss

**Single trade:**

- Planned principal is 0.05 SOL.
- A complete rug or unsellable honeypot can lose approximately **0.05 SOL**, not merely the configured 0.005 SOL hard-stop amount.
- Add transaction fees, priority fees, rent that has not been reclaimed, and any tip. A practical all-in one-trade budget is **0.051-0.052 SOL** under ordinary fee conditions; a failed/retried active-period trade can cost more.
- If slippage is set to 20%, a bad but successful fill can lose substantially before the hard-stop logic reacts. The protocol-level maximum is not the strategy's desired maximum.

**Single day:**

- With four concurrent positions, planned exposure is 0.20 SOL.
- If all four positions rug or cannot exit, the day can lose approximately **0.20 SOL plus fees**, even if the configured per-position stop is 10%.
- A safer first-live daily loss cap is **0.10 SOL**, with a hard maximum exposure of 0.20 SOL and no new entries when a sell is unresolved.
- The hot wallet should be funded with 1.0 SOL but the risk engine should not interpret wallet balance as available risk. Keep most of it as a reserve.

## Recommended Go/No-Go Sequence

1. **No-go for unrestricted live trading.** The 5% stress case is already slightly negative, and 16/20 newest sample rows lack recorded entry liquidity.
2. Add quote/fill telemetry before enabling live mode: Jupiter route, impact, expected output, actual output, fees, signature, confirmation latency, and wallet-balance reconciliation.
3. Run a shadow phase that obtains Jupiter quotes without signing, comparing DexScreener marks with executable route prices.
4. Run one guarded micro-live trade at 0.005-0.01 SOL, not 0.05 SOL, with a dedicated wallet, daily cap, and human confirmation.
5. Require at least 20 successful entry/exit observations before increasing to 0.05 SOL. Review median/p90 round-trip friction and failed-exit rate, not only PnL.
6. Keep live trading closed if median round-trip friction is above 2.5%, p90 is above 5%, or any unresolved sell remains open.

## Sources

Official and primary sources consulted:

- Jupiter developer documentation index: <https://developers.jup.ag/docs/llms.txt>
- Jupiter current Swap API overview: <https://developers.jup.ag/docs/swap>
- Jupiter V2 Order and Execute: <https://developers.jup.ag/docs/swap/order-and-execute>
- Jupiter V2 Build: <https://developers.jup.ag/docs/swap/build>
- Jupiter transaction submission: <https://developers.jup.ag/docs/transaction/submit>
- Jupiter historical Metis/V6-style quote guide: <https://dev.jup.ag/docs/swap/v1/get-quote>
- Jupiter historical Metis/V6-style build guide: <https://developers.jup.ag/docs/swap/v1/build-swap-transaction>
- Jupiter historical Metis/V6-style send guide: <https://developers.jup.ag/docs/swap/v1/send-swap-transaction>
- Jupiter V1 OpenAPI schema: <https://raw.githubusercontent.com/jup-ag/jupiter-quote-api-node/main/swagger.yaml>
- Solana fee documentation: <https://solana.com/docs/core/fees>
- Solana `sendTransaction`: <https://solana.com/docs/rpc/http/sendtransaction>
- Solana `getSignatureStatuses`: <https://solana.com/docs/rpc/http/getsignaturestatuses>
- Solana `getTransaction`: <https://solana.com/docs/rpc/http/gettransaction>
- DexScreener API reference: <https://docs.dexscreener.com/api/reference.md>
- Jito low-latency transaction and bundle documentation: <https://docs.jito.wtf/lowlatencytxnsend/>
- `jupiter-python-sdk`: <https://github.com/0xTaoDev/jupiter-python-sdk>
- `jupiter-python-sdk` PyPI release metadata: <https://pypi.org/project/jupiter-python-sdk/>
- `jito-py-rpc`: <https://github.com/jito-labs/jito-py-rpc>

Local sources consulted:

- `scripts/run_strategy_b.py`
- `src/execution/paper.py`
- `src/execution/jupiter_live.py`
- `src/execution/live_execution_config.py`
- `src/chain/jito.py`
- `src/strategy/exits.py`
- `analysis/strategy_b_entry_quality.md`
- Read-only snapshot/query of `data/trades.db`

## Method Limits

- The database was live and continued changing after the query. Reported counts are a time-bounded snapshot, not a permanent historical total.
- Paper PnL is not live fill PnL. The stress model is symmetric and does not reproduce route-level AMM math.
- DexScreener's public endpoints were used only to verify that current pair data exists; current liquidity was not substituted for historical entry liquidity.
- No private key, wallet, Jupiter swap, transaction submission, or code path was executed.
