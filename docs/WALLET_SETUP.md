# Safe Disposable MT Hot Wallet Setup

This guide covers creating a safe disposable hot wallet for the guarded
micro-live smoke test. The key principle: **never risk your personal/main
wallet.** Use a fresh wallet created only for this test, fund it with
tiny disposable SOL only, and remove the private key immediately after.

## Why Not Use Your Personal/Main Wallet

- The MT system constructs and signs transactions programmatically.
  A bug, misconfiguration, or compromised dependency could drain your
  main wallet.
- Private keys are stored in `.env` — a file that has historically been
  accidentally committed, pasted into logs, or exposed in debug output.
- The smoke test's purpose is to validate the full pipeline with real
  SOL at risk. Use the smallest possible amount, in a wallet that can
  be discarded if something goes wrong.

## Creating a Fresh Wallet

### Option A: Phantom / Solflare / Backpack (Recommended for non-technical)

1. Install the browser extension (Phantom, Solflare, or Backpack).
2. Create a **new wallet** (do not import existing seed phrase).
3. Save the seed phrase securely offline.
4. Copy the public wallet address.
5. Fund the wallet with a tiny amount of SOL (e.g. 0.01–0.1 SOL) from
   an exchange or your main wallet.
6. Only export/access the private key at the moment it is needed for
   the smoke test (step 4 in the procedure below).

### Option B: Solana CLI (Recommended for technical)

```bash
# Generate a new keypair
solana-keygen new --no-outfile --force

# The output shows:
#   pubkey: <PUBLIC_KEY>
#   seed phrase: <12 WORDS>

# To extract the private key (base58) when needed:
solana-keygen recover 'prompt:?key=0/0' --force
# Enter the 12-word seed phrase when prompted
# The output includes both pubkey and private key

# Fund the wallet (from exchange or another wallet)
solana transfer --from <SOURCE> <PUBLIC_KEY> 0.05 --allow-unfunded-recipient

# Check balance
solana balance <PUBLIC_KEY>
```

## Env Progression

Add environment variables to `.env` in a specific order. Never add them
all at once unless you are actively executing the live smoke test.

### Phase 1 — Read-only checks (current state)

Only `HELIUS_API_KEY` is set. This enables:
- `transaction_simulator` — available
- `env-readiness` — reports HELIUS_API_KEY=present

### Phase 2 — Add public key (read-only wallet checks)

```
TRADING_WALLET_PUBLIC_KEY=<your_fresh_wallet_public_key>
```

This enables:
- `wallet_balance_lookup` — available
- `wallet_holdings_lookup` — available
- `position_reconciliation` — operational
- `live-readiness` — preflight and reconciliation checks go green
- Still no signing capability — no risk of unintended transactions

### Phase 3 — Fund the wallet

Send 0.01–0.1 SOL (no more) to the public key address.
Verify with:
```bash
python3 -m src.cli live-readiness
# preflight should show ok (wallet balance above reserve minimum)
```

### Phase 4 — Add live arming vars

```
LIVE_TRADING_ENABLED=true
LIVE_CONFIRMATION_PHRASE=I_UNDERSTAND_THIS_CAN_LOSE_REAL_SOL
LIVE_KILL_SWITCH=false
MAX_LIVE_TRADE_SOL=0.005
MAX_LIVE_DAILY_TRADES=1
MAX_LIVE_DAILY_LOSS_SOL=0.02
PRIMARY_RPC_URL=https://mainnet.helius-rpc.com/?api-key=your_key
BACKUP_RPC_URL=https://mainnet.helius-rpc.com/?api-key=your_key
```

At this point `live-readiness` should show all checks ok except
wallet signing (private key missing).

### Phase 5 — Add private key (only for the smoke window)

Add `TRADING_WALLET_PRIVATE_KEY` to `.env` **immediately before** running
the smoke command, and remove it **immediately after**.

```
TRADING_WALLET_PRIVATE_KEY=<your_private_key>
```

## Critical Safety Rules

| Rule | Why |
|------|-----|
| Never commit `.env` | Private keys would be pushed to the remote. |
| Never paste private key into chat/logs | Plaintext exposure defeats the purpose. |
| Never mix personal wallet keys with MT | The MT `.env` file should only ever contain disposable wallet credentials. |
| Remove private key after smoke test | No wallet = no signing = no unintended transactions. |
| Remove live arming vars after smoke test | Prevents accidental live execution during normal paper-only operation. |
| Only fund with disposable SOL | If the wallet is drained, you lose at most 0.1 SOL. |
| Verify `.env` permissions | `chmod 600 .env` — readable only by the owner. |

## Removing Keys After Testing

After the smoke test completes, restore safe state:

```bash
# Remove private key
sed -i '/^TRADING_WALLET_PRIVATE_KEY=/d' .env

# Re-enable kill switch
sed -i 's/LIVE_KILL_SWITCH=false/LIVE_KILL_SWITCH=true/' .env

# Optionally remove live arming vars
sed -i '/^LIVE_TRADING_ENABLED=/d' .env
sed -i '/^LIVE_CONFIRMATION_PHRASE=/d' .env
sed -i '/^LIVE_KILL_SWITCH=/d' .env
sed -i '/^MAX_LIVE_TRADE_SOL=/d' .env
sed -i '/^MAX_LIVE_DAILY_TRADES=/d' .env
sed -i '/^MAX_LIVE_DAILY_LOSS_SOL=/d' .env

# Verify back to safe state
python3 -m src.cli live-readiness   # should show NOT READY
python3 -m src.cli env-readiness    # should show only HELIUS_API_KEY=MISSING or present
```
