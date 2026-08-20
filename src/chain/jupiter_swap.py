"""Live Jupiter swap execution client.

Builds, signs, sends, and confirms real swaps through the Jupiter Swap API
(``https://api.jup.ag/swap/v1``) with the API key from ``.env``. The wallet
private key from ``.env`` is decoded from base64 into a ``solders`` keypair.

This is the live execution path — every network interaction is injectable via
``httpx`` so tests never touch the real network.

Flow per swap:
1. ``get_quote`` — read-only price quote (SOL<->token, amounts in lamports).
2. ``execute_swap`` — POST /swap for the raw transaction, sign it with the
   wallet keypair, send via the Jito block engine as a single-transaction
   bundle with a tip (MT-589, when ``USE_JITO_BUNDLES`` is on) or via the RPC
   ``sendTransaction`` fallback, poll ``getSignatureStatuses`` for
   confirmation, rebuild with a fresh blockhash and retry when the blockhash
   expires (up to ``max_retries``).
3. Reconcile — always read the wallet token/SOL balance after the swap to
   verify the fill actually landed.

MT-589: Jito bundle routing. Every swap is wrapped as a bundle
``[tip_transfer, swap]`` submitted to the Jito block engine; the tip is
``max(dynamic_priority_fee, 0.001 SOL)`` paid to a randomly chosen canonical
tip account. A failed Jito submission logs a warning and falls back to the
plain RPC send — a bundle failure never blocks a trade. Flip
``USE_JITO_BUNDLES`` to False to disable.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

import httpx
from dotenv import load_dotenv
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import to_bytes_versioned
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction, VersionedTransaction

from src.chain.jito import JITO_TIP_ACCOUNTS, JitoBlockEngineClient
from src.chain.jupiter import LAMPORTS_PER_SOL, SOL_MINT
from src.chain.priority_fee import FeeCallback

load_dotenv()

log = logging.getLogger("jupiter_swap")

_DEFAULT_BASE_URL = "https://api.jup.ag"
_DEFAULT_SOLANA_RPC = "https://api.mainnet-beta.solana.com"
_FALLBACK_DECIMALS = 9
_PRIORITY_LEVEL = "veryHigh"
_PRIORITY_MAX_LAMPORTS = 1_000_000

# ── MT-589: Jito bundle routing ────────────────────────────────────────
# Master switch for Jito bundle submission of every swap. When True, each
# signed swap is wrapped in a bundle with a tip to one of the canonical Jito
# tip accounts below; on bundle failure the client falls back to the plain
# RPC sendTransaction path with a warning log.
USE_JITO_BUNDLES = True
JITO_BLOCK_ENGINE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"
# Minimum Jito tip: 0.001 SOL. The dynamic priority fee (75th percentile of
# getRecentPrioritizationFees, microlamports) is the base; the tip is
# max(dynamic_fee, MIN_JITO_TIP_LAMPORTS).
MIN_JITO_TIP_LAMPORTS = int(0.001 * LAMPORTS_PER_SOL)


@dataclass(frozen=True, slots=True)
class JupiterSwapQuote:
    """Normalized result of one Jupiter Swap API quote."""

    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price_impact_pct: float
    slippage_bps: int
    token_decimals: int
    price_sol: float | None
    raw: dict[str, object]
    quoted_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


@dataclass(frozen=True, slots=True)
class JupiterSwapResult:
    """Structured outcome of one live swap attempt."""

    ok: bool
    signature: str | None
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price_sol: float | None
    fees_lamports: int | None
    confirmation_status: str
    slot: int | None
    attempts: int
    error: str | None = None
    diagnostics: tuple[str, ...] = ()
    token_balance_after: float | None = None


class JupiterSwapClient:
    """Live Jupiter Swap API client with injectable HTTP and RPC transport."""

    def __init__(
        self,
        base_url: str = _DEFAULT_BASE_URL,
        solana_rpc_url: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        keypair: Keypair | None = None,
        api_key: str | None = None,
        timeout_s: float = 15.0,
        confirm_timeout_s: float = 30.0,
        poll_interval_s: float = 1.0,
        max_retries: int = 2,
        priority_fee_callback: FeeCallback | None = None,
        use_jito_bundles: bool | None = None,
        jito_client: JitoBlockEngineClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._solana_rpc_url = (
            solana_rpc_url
            or os.environ.get("PRIMARY_RPC_URL")
            or _DEFAULT_SOLANA_RPC
        )
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)
        self._keypair = keypair if keypair is not None else self._load_keypair()
        self._api_key = api_key if api_key is not None else os.environ.get("JUPITER_API_KEY")
        if not self._api_key:
            raise RuntimeError("JUPITER_API_KEY is missing from the environment")
        self._confirm_timeout_s = confirm_timeout_s
        self._poll_interval_s = poll_interval_s
        self._max_retries = max_retries
        self._decimals_cache: dict[str, int] = {}
        self._priority_fee_callback = priority_fee_callback
        # MT-589: Jito bundle routing. `use_jito_bundles` defaults to the
        # module-level USE_JITO_BUNDLES flag; the Jito client shares this
        # client's HTTP transport so tests never touch the real network.
        self._use_jito_bundles = (
            USE_JITO_BUNDLES if use_jito_bundles is None else use_jito_bundles
        )
        self._jito_client = jito_client or JitoBlockEngineClient(
            endpoint=JITO_BLOCK_ENGINE_URL,
            http_client=self._client,
        )

    @staticmethod
    def _load_keypair() -> Keypair:
        secret = os.environ.get("WALLET_PRIVATE_KEY")
        if not secret:
            raise RuntimeError("WALLET_PRIVATE_KEY is missing from the environment")
        try:
            decoded = base64.b64decode(secret)
        except Exception as exc:
            raise RuntimeError("WALLET_PRIVATE_KEY is not valid base64") from exc
        if len(decoded) != 64:
            raise RuntimeError(f"WALLET_PRIVATE_KEY must decode to 64 bytes, got {len(decoded)}")
        return Keypair.from_bytes(decoded)

    @property
    def wallet_pubkey(self) -> str:
        return str(self._keypair.pubkey())

    # ── Token metadata ───────────────────────────────────────────────

    async def get_token_decimals(self, mint: str) -> int:
        """Cached token decimals via public RPC ``getTokenSupply`` (9 fallback)."""
        if mint in self._decimals_cache:
            return self._decimals_cache[mint]
        if mint == SOL_MINT:
            self._decimals_cache[mint] = 9
            return 9
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [mint],
        }
        try:
            response = await self._client.post(self._solana_rpc_url, json=payload)
            response.raise_for_status()
            data = response.json()
            decimals = int(data["result"]["value"]["decimals"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            log.warning("LIVE decimals lookup failed for %s — fallback %d: %s",
                        mint[:16], _FALLBACK_DECIMALS, exc)
            decimals = _FALLBACK_DECIMALS
        self._decimals_cache[mint] = decimals
        return decimals

    # ── Quote ────────────────────────────────────────────────────────

    async def get_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
        slippage_bps: int = 100,
    ) -> JupiterSwapQuote | None:
        """Fetch one live quote. Returns ``None`` on any failure — never raises."""
        if amount_lamports <= 0:
            log.warning("LIVE quote rejected non-positive amount %d", amount_lamports)
            return None

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "slippageBps": str(slippage_bps),
            "dynamicSlippage": "true",
        }
        try:
            response = await self._client.get(
                f"{self._base_url}/swap/v1/quote",
                params=params,
                headers=self._headers(),
            )
        except httpx.HTTPError as exc:
            log.warning("LIVE quote request failed %s→%s: %s",
                        input_mint[:16], output_mint[:16], exc)
            return None

        if response.status_code == 429:
            log.warning("LIVE quote rate limited (429) %s→%s",
                        input_mint[:16], output_mint[:16])
            return None
        if response.status_code != 200:
            log.warning("LIVE quote failed HTTP %d %s→%s: %s",
                        response.status_code, input_mint[:16], output_mint[:16],
                        response.text[:200])
            return None

        try:
            data = response.json()
            in_amount = int(data["inAmount"])
            out_amount = int(data["outAmount"])
            price_impact_pct = float(data.get("priceImpactPct", 0.0))
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("LIVE malformed quote response %s→%s: %s",
                        input_mint[:16], output_mint[:16], exc)
            return None

        token_mint = output_mint if input_mint == SOL_MINT else input_mint
        decimals = await self.get_token_decimals(token_mint)
        price_sol = self._derive_price_sol(
            input_mint == SOL_MINT, in_amount, out_amount, decimals,
        )
        return JupiterSwapQuote(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount=in_amount,
            out_amount=out_amount,
            price_impact_pct=price_impact_pct,
            slippage_bps=slippage_bps,
            token_decimals=decimals,
            price_sol=price_sol,
            raw=data,
        )

    # ── Swap execution ───────────────────────────────────────────────

    async def execute_swap(self, quote: JupiterSwapQuote) -> JupiterSwapResult:
        """Sign, send, confirm, and reconcile one swap.

        Retries with a fresh blockhash up to ``max_retries`` times when the
        blockhash expires before confirmation. Always reconciles the wallet
        balance afterwards, even on failure.
        """
        last_error: str | None = None
        diagnostics: list[str] = []
        attempts = 0

        while attempts <= self._max_retries:
            attempts += 1
            diagnostics.append(f"attempt_{attempts}")
            try:
                result = await self._execute_single_attempt(quote, diagnostics)
            except Exception as exc:
                log.error("LIVE swap attempt %d crashed: %s", attempts, exc)
                last_error = f"swap crash: {exc}"
                diagnostics.append("attempt_crashed")
                break

            if result.ok:
                return replace(result, attempts=attempts)
            if result.confirmation_status == "failed":
                return replace(result, attempts=attempts)
            last_error = result.error
            if attempts > self._max_retries:
                break

            log.warning(
                "LIVE swap %s expired — rebuilding with fresh blockhash "
                "(attempt %d/%d)",
                result.signature or "?", attempts, self._max_retries,
            )
            fresh = await self.get_quote(
                quote.input_mint, quote.output_mint, quote.in_amount, quote.slippage_bps,
            )
            if fresh is None:
                log.error("LIVE requote failed during retry — aborting")
                return await self._fail_result(
                    quote, attempts, "expired", last_error,
                    diagnostics + ["requote_failed"],
                )
            quote = fresh

        return await self._fail_result(
            quote, attempts, "expired", last_error or "blockhash expired",
            diagnostics,
        )

    async def _execute_single_attempt(
        self,
        quote: JupiterSwapQuote,
        diagnostics: list[str],
    ) -> JupiterSwapResult:
        swap_transaction, last_valid_block_height, fees = await self._request_swap_transaction(
            quote,
        )
        diagnostics.append("swap_transaction_ready")

        signed_b64 = self._sign_transaction(swap_transaction)
        diagnostics.append("transaction_signed")

        signature = await self._send_transaction(signed_b64)
        if signature is None:
            return self._fail_result(quote, 1, "failed", "sendTransaction failed", diagnostics)
        diagnostics.append(f"signature={signature}")

        confirmation = await self._confirm_signature(signature, last_valid_block_height)
        diagnostics.append(f"confirmation={confirmation.status}")

        if confirmation.status == "confirmed":
            balance = await self._reconcile_balance(quote.output_mint, quote.token_decimals)
            return JupiterSwapResult(
                ok=True,
                signature=signature,
                input_mint=quote.input_mint,
                output_mint=quote.output_mint,
                in_amount=quote.in_amount,
                out_amount=quote.out_amount,
                price_sol=quote.price_sol,
                fees_lamports=fees,
                confirmation_status="confirmed",
                slot=confirmation.slot,
                attempts=1,
                diagnostics=tuple(diagnostics),
                token_balance_after=balance,
            )

        if confirmation.status == "failed":
            balance = await self._reconcile_balance(quote.output_mint, quote.token_decimals)
            return JupiterSwapResult(
                ok=False,
                signature=signature,
                input_mint=quote.input_mint,
                output_mint=quote.output_mint,
                in_amount=quote.in_amount,
                out_amount=quote.out_amount,
                price_sol=quote.price_sol,
                fees_lamports=fees,
                confirmation_status="failed",
                slot=None,
                attempts=1,
                error=confirmation.error,
                diagnostics=tuple(diagnostics),
                token_balance_after=balance,
            )

        return JupiterSwapResult(
            ok=False,
            signature=signature,
            input_mint=quote.input_mint,
            output_mint=quote.output_mint,
            in_amount=quote.in_amount,
            out_amount=quote.out_amount,
            price_sol=quote.price_sol,
            fees_lamports=fees,
            confirmation_status="expired",
            slot=None,
            attempts=1,
            error=confirmation.error or "blockhash expired before confirmation",
            diagnostics=tuple(diagnostics),
        )

    async def _request_swap_transaction(
        self,
        quote: JupiterSwapQuote,
    ) -> tuple[str, int | None, int | None]:
        """POST /swap and return (base64 tx, lastValidBlockHeight, fees)."""
        body = {
            "quoteResponse": quote.raw,
            "userPublicKey": self.wallet_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicSlippage": True,
        }
        body["prioritizationFeeLamports"] = await self._prioritization_fee_body()
        try:
            response = await self._client.post(
                f"{self._base_url}/swap/v1/swap",
                json=body,
                headers=self._headers(),
            )
        except httpx.HTTPError as exc:
            raise RuntimeError(f"swap request failed: {exc}") from exc

        if response.status_code != 200:
            raise RuntimeError(
                f"swap request failed HTTP {response.status_code}: {response.text[:200]}",
            )
        try:
            data = response.json()
            swap_transaction = str(data["swapTransaction"])
        except (ValueError, KeyError, TypeError) as exc:
            raise RuntimeError(f"malformed swap response: {exc}") from exc

        last_valid: int | None = None
        try:
            last_valid = int(data["lastValidBlockHeight"])
        except (KeyError, ValueError, TypeError):
            last_valid = None

        fees = self._parse_fees(data.get("fees"))
        if fees is None:
            try:
                fees = int(data["prioritizationFeeLamports"])
            except (KeyError, ValueError, TypeError):
                fees = None
        return swap_transaction, last_valid, fees

    async def _prioritization_fee_body(self) -> int | dict[str, object]:
        """MT-588: dynamic priority fee from the RPC when a callback is wired.

        Returns the exact lamport fee when the callback provides one (75th
        percentile of recent RPC fees, cached 30s). Falls back to Jupiter's
        ``priorityLevelWithMaxLamports`` (veryHigh, 1M cap) otherwise — the
        previous static behavior — so a fee-lookup failure never blocks a swap.
        """
        if self._priority_fee_callback is not None:
            try:
                fee_lamports = await self._priority_fee_callback()
            except Exception as exc:  # noqa: BLE001 — degrade, never block a trade
                log.warning("LIVE dynamic priority fee lookup failed: %s", exc)
                fee_lamports = None
            if fee_lamports is not None and fee_lamports > 0:
                log.info("LIVE swap priority fee lamports=%d (dynamic)", fee_lamports)
                return fee_lamports
        return {
            "priorityLevelWithMaxLamports": {
                "priorityLevel": _PRIORITY_LEVEL,
                "maxLamports": _PRIORITY_MAX_LAMPORTS,
                "global": False,
            },
        }

    def _sign_transaction(self, swap_transaction_b64: str) -> str:
        """Decode the base64 versioned transaction, sign with the wallet keypair."""
        try:
            unsigned = VersionedTransaction.from_bytes(base64.b64decode(swap_transaction_b64))
            signature = self._keypair.sign_message(to_bytes_versioned(unsigned.message))
            signed = VersionedTransaction.populate(unsigned.message, [signature])
            return base64.b64encode(bytes(signed)).decode()
        except Exception as exc:
            raise RuntimeError(f"transaction signing failed: {exc}") from exc

    async def _send_transaction(self, signed_b64: str) -> str | None:
        """Send the signed swap via Jito bundle (MT-589) or plain RPC.

        Returns the swap transaction signature on success, ``None`` on
        failure. With Jito enabled the bundle ``[tip_transfer, swap]`` is
        submitted first; any bundle failure logs a warning and falls back to
        the standard RPC ``sendTransaction`` path.
        """
        if self._use_jito_bundles:
            bundle_signature = await self._send_via_jito(signed_b64)
            if bundle_signature is not None:
                return bundle_signature
        return await self._send_via_rpc(signed_b64)

    async def _send_via_jito(self, signed_b64: str) -> str | None:
        """Submit the signed swap as a one-transaction Jito bundle with a tip.

        The tip transfer uses the same recent blockhash as the swap (Jito
        requires one blockhash per bundle) and pays
        ``max(dynamic_priority_fee, 0.001 SOL)`` to a randomly chosen
        canonical tip account. Returns the swap signature when the bundle is
        accepted, ``None`` on any failure (the caller falls back to RPC).
        """
        try:
            signed_bytes = base64.b64decode(signed_b64)
            swap_signature = str(Signature.from_bytes(signed_bytes[:64]))
            blockhash = VersionedTransaction.from_bytes(signed_bytes).message.recent_blockhash
            tip_lamports = await self._jito_tip_lamports()
            tip_account = random.choice(JITO_TIP_ACCOUNTS)
            tip_tx_b64 = self._build_tip_transaction(tip_lamports, tip_account, blockhash)
            result = await self._jito_client._submit_bundle_for_guarded_adapter(
                [tip_tx_b64, signed_b64],
            )
        except Exception as exc:  # noqa: BLE001 — a bundle failure never blocks a trade
            log.warning("JITO bundle submission failed: %s — falling back to RPC send", exc)
            return None
        if not result.ok:
            log.warning(
                "JITO bundle submission failed: %s — falling back to RPC send",
                result.error or "unknown",
            )
            return None
        log.info(
            "JITO bundle submitted bundle_id=%s tip_lamports=%d tip_account=%s sig=%s",
            result.bundle_id, tip_lamports, tip_account[:8], swap_signature,
        )
        return swap_signature

    async def _jito_tip_lamports(self) -> int:
        """MT-589: Jito tip = max(dynamic priority fee, 0.001 SOL)."""
        dynamic = 0
        if self._priority_fee_callback is not None:
            try:
                fee = await self._priority_fee_callback()
                if fee is not None and fee > 0:
                    dynamic = fee
            except Exception as exc:  # noqa: BLE001 — degrade to the minimum tip
                log.warning("JITO tip dynamic fee lookup failed: %s", exc)
        return max(dynamic, MIN_JITO_TIP_LAMPORTS)

    def _build_tip_transaction(self, tip_lamports: int, tip_account: str, blockhash: Hash) -> str:
        """Build a signed legacy SOL transfer to a Jito tip account (base64).

        The tip transfer shares the swap transaction's recent blockhash —
        Jito bundles require a single blockhash across all transactions.
        """
        instruction = transfer(
            TransferParams(
                from_pubkey=self._keypair.pubkey(),
                to_pubkey=Pubkey.from_string(tip_account),
                lamports=tip_lamports,
            ),
        )
        transaction = Transaction.new_signed_with_payer(
            [instruction], self._keypair.pubkey(), [self._keypair], blockhash,
        )
        return base64.b64encode(bytes(transaction)).decode()

    async def _send_via_rpc(self, signed_b64: str) -> str | None:
        """Send the signed transaction via the RPC ``sendTransaction`` method."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                signed_b64,
                {
                    "encoding": "base64",
                    "skipPreflight": False,
                    "preflightCommitment": "confirmed",
                    "maxRetries": 3,
                },
            ],
        }
        try:
            response = await self._client.post(self._solana_rpc_url, json=payload)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            log.error("LIVE sendTransaction failed: %s", exc)
            return None
        result = data.get("result")
        if isinstance(result, str):
            return result
        log.error("LIVE sendTransaction returned no signature: %s", data.get("error"))
        return None

    async def _confirm_signature(
        self,
        signature: str,
        last_valid_block_height: int | None,
    ) -> _Confirmation:
        """Poll ``getSignatureStatuses`` up to ``confirm_timeout_s``."""
        deadline = time.monotonic() + self._confirm_timeout_s
        while True:
            status = await self._signature_status(signature)
            if status is not None:
                return status
            if time.monotonic() >= deadline:
                return _Confirmation(status="expired", slot=None, error=None)
            await asyncio.sleep(self._poll_interval_s)

    async def _signature_status(self, signature: str) -> _Confirmation | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [[signature], {"searchTransactionHistory": True}],
        }
        try:
            response = await self._client.post(self._solana_rpc_url, json=payload)
            response.raise_for_status()
            data = response.json()
            value = data["result"]["value"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            log.warning("LIVE getSignatureStatuses failed: %s", exc)
            return None
        if not value or value[0] is None:
            return None
        entry = value[0]
        if entry.get("err") is not None:
            return _Confirmation(
                status="failed",
                slot=None,
                error=f"transaction failed: {entry.get('err')}",
            )
        if entry.get("confirmationStatus") in ("confirmed", "finalized") or entry.get("slot"):
            return _Confirmation(status="confirmed", slot=entry.get("slot"), error=None)
        return None

    async def _reconcile_balance(self, mint: str, token_decimals: int) -> float | None:
        """Read the wallet balance of ``mint`` to verify the fill landed."""
        try:
            if mint == SOL_MINT:
                lamports = await self._sol_balance_lamports()
                return lamports / LAMPORTS_PER_SOL if lamports is not None else None
            raw = await self._token_balance_raw(mint)
            return raw / (10**token_decimals) if raw is not None else None
        except Exception as exc:
            log.warning("LIVE balance reconciliation failed for %s: %s", mint[:16], exc)
            return None

    async def get_sol_balance(self) -> float | None:
        """Wallet SOL balance in SOL, or ``None`` on failure."""
        lamports = await self._sol_balance_lamports()
        return lamports / LAMPORTS_PER_SOL if lamports is not None else None

    async def get_token_balance(self, mint: str) -> float | None:
        """Wallet token balance in token units, or ``None`` on failure."""
        decimals = await self.get_token_decimals(mint)
        raw = await self._token_balance_raw(mint)
        return raw / (10**decimals) if raw is not None else None

    async def _sol_balance_lamports(self) -> int | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [self.wallet_pubkey],
        }
        try:
            response = await self._client.post(self._solana_rpc_url, json=payload)
            response.raise_for_status()
            return int(response.json()["result"]["value"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            return None

    async def _token_balance_raw(self, mint: str) -> int | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                self.wallet_pubkey,
                {"mint": mint},
                {"encoding": "jsonParsed"},
            ],
        }
        try:
            response = await self._client.post(self._solana_rpc_url, json=payload)
            response.raise_for_status()
            accounts = response.json()["result"]["value"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            return None
        total = 0
        for account in accounts:
            try:
                amount = account["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
                total += int(amount)
            except (KeyError, ValueError, TypeError):
                continue
        return total

    # ── Helpers ──────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self._api_key}

    @staticmethod
    def _parse_fees(fees: object) -> int | None:
        """Sum Jupiter-reported fee lamports when present."""
        if not isinstance(fees, dict):
            return None
        total = 0
        found = False
        for key in ("signatureFee", "openOrdersDeposits", "ataDeposits", "totalFeeAndDeposits"):
            value = fees.get(key)
            if isinstance(value, dict):
                try:
                    total += int(value["amount"])
                    found = True
                except (KeyError, ValueError, TypeError):
                    continue
            elif isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict):
                        try:
                            total += int(entry["amount"])
                            found = True
                        except (KeyError, ValueError, TypeError):
                            continue
        return total if found else None

    def _derive_price_sol(
        self,
        sol_in: bool,
        in_amount: int,
        out_amount: int,
        token_decimals: int,
    ) -> float | None:
        if sol_in:
            token_amount = out_amount / (10**token_decimals)
            sol_amount = in_amount / LAMPORTS_PER_SOL
        else:
            token_amount = in_amount / (10**token_decimals)
            sol_amount = out_amount / LAMPORTS_PER_SOL
        if token_amount <= 0:
            return None
        return sol_amount / token_amount

    async def _fail_result(
        self,
        quote: JupiterSwapQuote,
        attempts: int,
        status: str,
        error: str | None,
        diagnostics: list[str],
    ) -> JupiterSwapResult:
        balance = await self._reconcile_balance(quote.output_mint, quote.token_decimals)
        return JupiterSwapResult(
            ok=False,
            signature=None,
            input_mint=quote.input_mint,
            output_mint=quote.output_mint,
            in_amount=quote.in_amount,
            out_amount=quote.out_amount,
            price_sol=quote.price_sol,
            fees_lamports=None,
            confirmation_status=status,
            slot=None,
            attempts=attempts,
            error=error,
            diagnostics=tuple(diagnostics),
            token_balance_after=balance,
        )

    async def close(self) -> None:
        await self._client.aclose()


@dataclass(frozen=True, slots=True)
class _Confirmation:
    status: str
    slot: int | None
    error: str | None
