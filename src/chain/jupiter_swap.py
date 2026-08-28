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

MT-636: swaps use the QuickNode Metis add-on when ``QUICKNODE_RPC_URL`` is
configured and fall back to public Jupiter. Jito bundles use the QuickNode
Lil' JIT ``getTipFloor`` API's EMA median, clamped to 0.0005 SOL.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import math
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

_PUBLIC_JUPITER_BASE_URL = "https://public.jupiterapi.com"
_DEFAULT_SOLANA_RPC = "https://api.mainnet-beta.solana.com"
_FALLBACK_DECIMALS = 9
_PRIORITY_LEVEL = "veryHigh"
_PRIORITY_MAX_LAMPORTS = 1_000_000
_MAX_TRANSACTION_SIZE_BYTES = 1_232
_COMPACT_ROUTE_MAX_ACCOUNTS = 32
TOKEN_PROGRAM_IDS = (
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
)

# ── MT-636: Jito bundle routing ────────────────────────────────────────
# Master switch for Jito bundle submission. The size threshold below keeps
# small swaps on direct RPC even when this switch is enabled.
USE_JITO_BUNDLES = True
DEFAULT_JITO_MIN_SIZE_SOL = 0.1
JITO_BLOCK_ENGINE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"
# Jito requires a minimum 1,000-lamport bundle tip. The smart tip is drawn
# from getTipFloor and capped so a busy auction cannot consume trade PnL.
MIN_JITO_TIP_LAMPORTS = 1_000
MAX_JITO_TIP_LAMPORTS = int(0.0005 * LAMPORTS_PER_SOL)
_JITO_TIP_CACHE_SECONDS = 30.0


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
    swap_api_base_url: str = ""
    swap_api_legacy_paths: bool = False
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


@dataclass(frozen=True, slots=True)
class TokenAccountBalance:
    """One SPL or Token-2022 account owned by the trading wallet."""

    address: str
    mint: str
    raw_amount: int
    decimals: int
    program_id: str


class _TransactionTooLargeError(RuntimeError):
    """Raised when Jupiter cannot build a swap within Solana's byte limit."""


class JupiterSwapClient:
    """Live Jupiter Swap API client with injectable HTTP and RPC transport."""

    def __init__(
        self,
        base_url: str | None = None,
        solana_rpc_url: str | None = None,
        backup_solana_rpc_url: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        keypair: Keypair | None = None,
        api_key: str | None = None,
        timeout_s: float = 15.0,
        confirm_timeout_s: float = 30.0,
        poll_interval_s: float = 1.0,
        max_retries: int = 2,
        priority_fee_callback: FeeCallback | None = None,
        use_jito_bundles: bool | None = None,
        jito_min_size_sol: float | None = None,
        jito_client: JitoBlockEngineClient | None = None,
    ) -> None:
        # Explicit base_url preserves the old Jupiter /swap/v1 test and
        # operator override surface. Normal runtime uses QuickNode Metis.
        self._metis_base_url = (
            base_url.rstrip("/")
            if base_url
            else os.environ.get("QUICKNODE_RPC_URL", "").rstrip("/")
        )
        self._metis_legacy_paths = base_url is not None
        self._public_jupiter_base_url = _PUBLIC_JUPITER_BASE_URL
        self._solana_rpc_url = (
            solana_rpc_url
            or os.environ.get("QUICKNODE_RPC_URL")
            or os.environ.get("PRIMARY_RPC_URL")
            or _DEFAULT_SOLANA_RPC
        )
        self._backup_solana_rpc_url = (
            backup_solana_rpc_url
            or os.environ.get("BACKUP_RPC_URL")
            or os.environ.get("HELIUS_RPC_URL")
            or None
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
        self._jito_tip_lamports_cache: int | None = None
        self._jito_tip_fetched_at = 0.0
        # MT-589: Jito bundle routing. `use_jito_bundles` defaults to the
        # module-level USE_JITO_BUNDLES flag; the Jito client shares this
        # client's HTTP transport so tests never touch the real network.
        self._use_jito_bundles = (
            USE_JITO_BUNDLES if use_jito_bundles is None else use_jito_bundles
        )
        self._jito_min_size_sol = self._resolve_jito_min_size_sol(jito_min_size_sol)
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
            response = await self._rpc_post(payload)
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
        max_accounts: int | None = None,
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
        if max_accounts is not None:
            params["maxAccounts"] = str(max_accounts)
        try:
            response, swap_api_base_url, swap_api_legacy_paths = await self._get_swap_response(
                "quote", params=params,
            )
        except httpx.HTTPError as exc:
            log.warning("LIVE quote request failed %s→%s: %s",
                        input_mint[:16], output_mint[:16], exc)
            return None

        if response is None or response.status_code != 200:
            status = response.status_code if response is not None else "unavailable"
            log.warning("LIVE quote failed HTTP %s %s→%s: %s",
                        status, input_mint[:16], output_mint[:16],
                        response.text[:200] if response is not None else "no provider response")
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
            swap_api_base_url=swap_api_base_url,
            swap_api_legacy_paths=swap_api_legacy_paths,
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
            except _TransactionTooLargeError as exc:
                log.error("LIVE swap not submitted: %s", exc)
                return await self._fail_result(
                    quote, attempts, "failed", str(exc), diagnostics + ["transaction_too_large"],
                )
            except Exception as exc:
                log.error("LIVE swap attempt %d crashed: %s", attempts, exc)
                last_error = f"swap crash: {exc}"
                diagnostics.append("attempt_crashed")
                break

            if result.ok:
                return replace(result, attempts=attempts)
            if result.confirmation_status == "failed":
                return replace(result, attempts=attempts)
            if result.confirmation_status == "unknown":
                # A submitted transaction may still land. Never replace it
                # until RPC proves the prior blockhash expired.
                return replace(result, attempts=attempts)
            last_error = result.error
            if attempts > self._max_retries:
                break

            # The status poll that timed out can race a late confirmation. A
            # fresh status check immediately before resubmission is mandatory.
            if result.signature is not None:
                latest = await self._signature_status(result.signature)
                if latest is not None and latest.status == "confirmed":
                    balance = await self._reconcile_balance(
                        quote.output_mint,
                        quote.token_decimals,
                    )
                    return replace(
                        result,
                        ok=True,
                        confirmation_status="confirmed",
                        slot=latest.slot,
                        attempts=attempts,
                        error=None,
                        token_balance_after=balance,
                    )
                if latest is not None and latest.status == "failed":
                    return replace(
                        result,
                        confirmation_status="failed",
                        attempts=attempts,
                        error=latest.error,
                    )

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
        quote, swap_transaction, last_valid_block_height, fees = (
            await self._request_sized_swap_transaction(quote)
        )
        diagnostics.append("swap_transaction_ready")

        signed_b64 = self._sign_transaction(swap_transaction)
        diagnostics.append("transaction_signed")

        signature = await self._send_transaction(signed_b64, quote)
        if signature is None:
            return await self._fail_result(
                quote,
                1,
                "failed",
                "sendTransaction failed",
                diagnostics,
            )
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
            confirmation_status=confirmation.status,
            slot=None,
            attempts=1,
            error=confirmation.error or f"transaction {confirmation.status} before confirmation",
            diagnostics=tuple(diagnostics),
            token_balance_after=balance,
        )

    async def _request_sized_swap_transaction(
        self,
        quote: JupiterSwapQuote,
    ) -> tuple[JupiterSwapQuote, str, int | None, int | None]:
        """Build a swap transaction and retry once with a compact Jupiter route.

        Jito bundles carry the tip as a separate transaction, so a swap over
        Solana's 1,232-byte limit originates in Jupiter's generated route.
        Restricting accounts only after detecting that condition avoids
        degrading normal route selection.
        """
        swap_transaction, last_valid_block_height, fees = await self._request_swap_transaction(
            quote,
        )
        size_bytes = self._serialized_transaction_size(swap_transaction)
        if size_bytes <= _MAX_TRANSACTION_SIZE_BYTES:
            return quote, swap_transaction, last_valid_block_height, fees

        log.warning(
            "LIVE Jupiter swap transaction size=%d exceeds %d bytes; retrying with maxAccounts=%d",
            size_bytes,
            _MAX_TRANSACTION_SIZE_BYTES,
            _COMPACT_ROUTE_MAX_ACCOUNTS,
        )
        compact_quote = await self.get_quote(
            quote.input_mint,
            quote.output_mint,
            quote.in_amount,
            quote.slippage_bps,
            max_accounts=_COMPACT_ROUTE_MAX_ACCOUNTS,
        )
        if compact_quote is None:
            raise _TransactionTooLargeError(
                f"Jupiter built a {size_bytes}-byte transaction and no compact route was available",
            )
        compact_transaction, compact_last_valid, compact_fees = (
            await self._request_swap_transaction(compact_quote)
        )
        compact_size_bytes = self._serialized_transaction_size(compact_transaction)
        if compact_size_bytes > _MAX_TRANSACTION_SIZE_BYTES:
            raise _TransactionTooLargeError(
                "Jupiter compact route is still "
                f"{compact_size_bytes} bytes (limit {_MAX_TRANSACTION_SIZE_BYTES})",
            )
        log.info(
            "LIVE Jupiter compact route reduced transaction size from %d to %d bytes",
            size_bytes,
            compact_size_bytes,
        )
        return compact_quote, compact_transaction, compact_last_valid, compact_fees

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
            response = await self._post_swap_response(
                "swap",
                body,
                quote.swap_api_base_url,
                quote.swap_api_legacy_paths,
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

    async def _send_transaction(self, signed_b64: str, quote: JupiterSwapQuote) -> str | None:
        """Send the signed swap via Jito bundle (MT-589) or plain RPC.

        Returns the swap transaction signature on success, ``None`` on
        failure. With Jito enabled the bundle ``[tip_transfer, swap]`` is
        submitted first; any bundle failure logs a warning and falls back to
        the standard RPC ``sendTransaction`` path.
        """
        if self._should_use_jito_bundles(quote):
            bundle_signature = await self._send_via_jito(signed_b64)
            if bundle_signature is not None:
                return bundle_signature
        return await self._send_via_rpc(signed_b64)

    def _should_use_jito_bundles(self, quote: JupiterSwapQuote) -> bool:
        """Use Jito only when the SOL notional meets the configured threshold."""
        if not self._use_jito_bundles:
            return False
        if quote.input_mint == SOL_MINT:
            notional_lamports = quote.in_amount
        elif quote.output_mint == SOL_MINT:
            notional_lamports = quote.out_amount
        else:
            log.info("JITO skipped: swap has no SOL notional; using RPC")
            return False
        notional_sol = notional_lamports / LAMPORTS_PER_SOL
        if notional_sol < self._jito_min_size_sol:
            log.info(
                "JITO skipped: notional_sol=%.9f below JITO_MIN_SIZE_SOL=%.9f; using RPC",
                notional_sol,
                self._jito_min_size_sol,
            )
            return False
        return True

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
                tip_lamports=tip_lamports,
                validator_tip_account=tip_account,
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
        """Return a cached, capped Jito tip from QuickNode's getTipFloor API."""
        if (
            self._jito_tip_lamports_cache is not None
            and time.monotonic() - self._jito_tip_fetched_at < _JITO_TIP_CACHE_SECONDS
        ):
            return self._jito_tip_lamports_cache

        payload = {"jsonrpc": "2.0", "id": 1, "method": "getTipFloor", "params": []}
        try:
            response = await self._rpc_post(payload)
            response.raise_for_status()
            floors = response.json()["result"]
            floor = floors[0]
            tip_sol = float(
                floor.get("ema_landed_tips_50th_percentile")
                or floor["landed_tips_50th_percentile"],
            )
            tip_lamports = int(tip_sol * LAMPORTS_PER_SOL)
            self._jito_tip_lamports_cache = max(
                MIN_JITO_TIP_LAMPORTS,
                min(tip_lamports, MAX_JITO_TIP_LAMPORTS),
            )
            self._jito_tip_fetched_at = time.monotonic()
            log.info("JITO smart tip lamports=%d", self._jito_tip_lamports_cache)
        except (httpx.HTTPError, KeyError, TypeError, ValueError, IndexError) as exc:
            log.warning("JITO smart tip lookup failed; using minimum: %s", exc)
            return MIN_JITO_TIP_LAMPORTS
        return self._jito_tip_lamports_cache

    @staticmethod
    def _serialized_transaction_size(transaction_b64: str) -> int:
        try:
            return len(base64.b64decode(transaction_b64, validate=True))
        except Exception as exc:
            raise _TransactionTooLargeError(
                f"Jupiter returned invalid base64 transaction: {exc}",
            ) from exc

    @staticmethod
    def _resolve_jito_min_size_sol(value: float | None) -> float:
        """Read a finite, non-negative Jito threshold without blocking swaps."""
        raw_value: object = value if value is not None else os.environ.get("JITO_MIN_SIZE_SOL")
        if raw_value is None or raw_value == "":
            return DEFAULT_JITO_MIN_SIZE_SOL
        try:
            threshold = float(raw_value)
        except (TypeError, ValueError):
            log.warning(
                "Invalid JITO_MIN_SIZE_SOL; using default %.3f SOL",
                DEFAULT_JITO_MIN_SIZE_SOL,
            )
            return DEFAULT_JITO_MIN_SIZE_SOL
        if not math.isfinite(threshold) or threshold < 0:
            log.warning(
                "Invalid JITO_MIN_SIZE_SOL; using default %.3f SOL",
                DEFAULT_JITO_MIN_SIZE_SOL,
            )
            return DEFAULT_JITO_MIN_SIZE_SOL
        return threshold

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
            response = await self._rpc_post(payload)
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
        """Poll a signature and retry only after proven blockhash expiry."""
        deadline = time.monotonic() + self._confirm_timeout_s
        while True:
            status = await self._signature_status(signature)
            if status is not None:
                return status
            if time.monotonic() >= deadline:
                # Recheck at the boundary before classifying the submission.
                status = await self._signature_status(signature)
                if status is not None:
                    return status
                if last_valid_block_height is not None:
                    block_height = await self._current_block_height()
                    if block_height is not None and block_height > last_valid_block_height:
                        return _Confirmation(
                            status="expired",
                            slot=None,
                            error="blockhash expired before confirmation",
                        )
                return _Confirmation(
                    status="unknown",
                    slot=None,
                    error="confirmation timed out before blockhash expiry was proven",
                )
            await asyncio.sleep(self._poll_interval_s)

    async def _signature_status(self, signature: str) -> _Confirmation | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [[signature], {"searchTransactionHistory": True}],
        }
        try:
            response = await self._rpc_post(payload)
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
        # A processed transaction also has a slot. Do not treat it as landed:
        # only confirmed/finalized status is sufficient to close a live position.
        if entry.get("confirmationStatus") in ("confirmed", "finalized"):
            return _Confirmation(status="confirmed", slot=entry.get("slot"), error=None)
        return None

    async def _current_block_height(self) -> int | None:
        """Return the current block height, or ``None`` when expiry is unknown."""

        payload = {"jsonrpc": "2.0", "id": 1, "method": "getBlockHeight", "params": []}
        try:
            response = await self._rpc_post(payload)
            response.raise_for_status()
            return int(response.json()["result"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            log.warning("LIVE getBlockHeight failed while checking expiry: %s", exc)
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

    async def get_wallet_holdings(self) -> dict[str, float] | None:
        """All positive SPL-token balances keyed by mint, or ``None`` on failure."""
        holdings: dict[str, float] = {}
        for program_id in TOKEN_PROGRAM_IDS:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    self.wallet_pubkey,
                    {"programId": program_id},
                    {"encoding": "jsonParsed"},
                ],
            }
            try:
                response = await self._rpc_post(payload)
                response.raise_for_status()
                accounts = response.json()["result"]["value"]
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                return None

            for account in accounts:
                try:
                    info = account["account"]["data"]["parsed"]["info"]
                    token_amount = info["tokenAmount"]
                    amount = int(token_amount["amount"]) / (10 ** int(token_amount["decimals"]))
                    mint = str(info["mint"])
                except (KeyError, ValueError, TypeError):
                    continue
                if amount > 0:
                    holdings[mint] = holdings.get(mint, 0.0) + amount
        return holdings

    async def get_token_accounts(self) -> list[TokenAccountBalance] | None:
        """Return SPL and Token-2022 accounts with raw balances for maintenance."""
        balances: list[TokenAccountBalance] = []
        for program_id in TOKEN_PROGRAM_IDS:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    self.wallet_pubkey,
                    {"programId": program_id},
                    {"encoding": "jsonParsed"},
                ],
            }
            try:
                response = await self._rpc_post(payload)
                response.raise_for_status()
                accounts = response.json()["result"]["value"]
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                return None

            for account in accounts:
                try:
                    info = account["account"]["data"]["parsed"]["info"]
                    token_amount = info["tokenAmount"]
                    raw_amount = int(token_amount["amount"])
                    if raw_amount <= 0:
                        continue
                    balances.append(
                        TokenAccountBalance(
                            address=str(account["pubkey"]),
                            mint=str(info["mint"]),
                            raw_amount=raw_amount,
                            decimals=int(token_amount["decimals"]),
                            program_id=program_id,
                        ),
                    )
                except (KeyError, ValueError, TypeError):
                    continue
        return balances

    async def burn_token_account(self, account: TokenAccountBalance) -> str | None:
        """Burn a standard SPL balance and return its transaction signature on confirmation.

        This is intentionally limited to accounts returned by ``get_token_accounts``;
        callers should use it only after an explicit human-confirmed dust cleanup.
        """
        from spl.token.instructions import burn
        from spl.token.models import BurnParams

        blockhash_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "confirmed"}],
        }
        try:
            response = await self._rpc_post(blockhash_payload)
            blockhash = Hash.from_string(response.json()["result"]["value"]["blockhash"])
            instruction = burn(
                BurnParams(
                    program_id=Pubkey.from_string(account.program_id),
                    mint=Pubkey.from_string(account.mint),
                    account=Pubkey.from_string(account.address),
                    owner=self._keypair.pubkey(),
                    amount=account.raw_amount,
                    signers=[],
                ),
            )
            transaction = Transaction.new_signed_with_payer(
                [instruction], self._keypair.pubkey(), [self._keypair], blockhash,
            )
            signed_b64 = base64.b64encode(bytes(transaction)).decode()
        except Exception as exc:  # This is an operator-only recovery path.
            log.warning("DUST burn build failed for %s: %s", account.mint[:16], exc)
            return None
        return await self._send_via_rpc(signed_b64)

    async def _sol_balance_lamports(self) -> int | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [self.wallet_pubkey],
        }
        try:
            response = await self._rpc_post(payload)
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
            response = await self._rpc_post(payload)
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

    async def _rpc_post(self, payload: dict[str, object]) -> httpx.Response:
        """Use QuickNode/primary RPC first and Helius/backup only on transport failure."""
        urls = [self._solana_rpc_url]
        if self._backup_solana_rpc_url and self._backup_solana_rpc_url != self._solana_rpc_url:
            urls.append(self._backup_solana_rpc_url)
        last_error: httpx.HTTPError | None = None
        for index, url in enumerate(urls):
            try:
                response = await self._client.post(url, json=payload)
                response.raise_for_status()
                return response
            except httpx.HTTPError as exc:
                last_error = exc
                if index + 1 < len(urls):
                    log.warning(
                        "LIVE RPC primary failed for %s; trying configured backup",
                        payload["method"],
                    )
        assert last_error is not None
        raise last_error

    async def _get_swap_response(
        self,
        endpoint: str,
        *,
        params: dict[str, str],
    ) -> tuple[httpx.Response | None, str, bool]:
        """Try QuickNode Metis first, then compatible public Jupiter."""
        providers: list[tuple[str, bool]] = []
        if self._metis_base_url:
            providers.append((self._metis_base_url, self._metis_legacy_paths))
        providers.append((self._public_jupiter_base_url, False))
        last_response: httpx.Response | None = None
        for base_url, legacy_paths in providers:
            try:
                response = await self._client.get(
                    self._swap_endpoint(base_url, endpoint, legacy_paths),
                    params=params,
                    headers=self._headers(),
                )
            except httpx.HTTPError:
                continue
            if response.status_code == 200:
                return response, base_url, legacy_paths
            last_response = response
        return last_response, self._public_jupiter_base_url, False

    async def _post_swap_response(
        self,
        endpoint: str,
        body: dict[str, object],
        quoted_base_url: str,
        quoted_legacy_paths: bool,
    ) -> httpx.Response:
        """Build through the quote's provider, retrying public Jupiter once."""
        base_url = quoted_base_url or self._metis_base_url or self._public_jupiter_base_url
        legacy_paths = quoted_legacy_paths if quoted_base_url else self._metis_legacy_paths
        providers = [(base_url, legacy_paths)]
        if base_url != self._public_jupiter_base_url:
            providers.append((self._public_jupiter_base_url, False))
        last_response: httpx.Response | None = None
        for provider_url, provider_legacy_paths in providers:
            try:
                response = await self._client.post(
                    self._swap_endpoint(provider_url, endpoint, provider_legacy_paths),
                    json=body,
                    headers=self._headers(),
                )
            except httpx.HTTPError:
                continue
            if response.status_code == 200:
                return response
            last_response = response
        if last_response is None:
            raise httpx.RequestError("all swap API providers failed")
        return last_response

    @staticmethod
    def _swap_endpoint(base_url: str, endpoint: str, legacy_paths: bool) -> str:
        path_prefix = "/swap/v1" if legacy_paths else ""
        return f"{base_url}{path_prefix}/{endpoint}"

    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self._api_key} if self._api_key else {}

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
