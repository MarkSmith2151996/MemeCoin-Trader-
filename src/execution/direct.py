"""Direct Pump.fun bonding-curve execution for active SOL-paired curves.

This adapter deliberately does not become the application default.  Callers
must select it explicitly, and the diagnostic script never submits unless its
``--live`` flag is provided.  Completed curves are rejected so routing can
fall back to PumpSwap/Jupiter instead of submitting a guaranteed failure.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from datetime import UTC, datetime, timedelta

import httpx
from dotenv import load_dotenv
from solders.hash import Hash
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction

from src.chain.jito import JITO_TIP_ACCOUNTS, JitoBlockEngineClient
from src.chain.jupiter import LAMPORTS_PER_SOL
from src.chain.pumpfun import (
    CurveCompleteError,
    calculate_buy_amount,
    calculate_sell_amount,
    fetch_bonding_curve_state,
    maximum_input,
    minimum_output,
)
from src.chain.pumpfun_tx import build_buy_instructions, build_sell_instruction
from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter
from src.execution.redaction import sanitize_provider_error

load_dotenv()

DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
DEFAULT_JITO_TIP_LAMPORTS = 1_000_000
MAX_TRANSACTION_SIZE_BYTES = 1_232
log = logging.getLogger("direct_executor")

# The direct adapter's Trade return type is the existing persisted trade model.
TradeResult = Trade


class DirectExecutor(ExecutionAdapter):
    """Build, sign, submit, and confirm Pump ``buy_v2``/``sell_v2`` swaps."""

    def __init__(
        self,
        *,
        rpc_url: str | None = None,
        keypair: Keypair | None = None,
        http_client: httpx.AsyncClient | None = None,
        jito_client: JitoBlockEngineClient | None = None,
        slippage_bps: int = 100,
        jito_tip_lamports: int = DEFAULT_JITO_TIP_LAMPORTS,
        use_jito: bool = False,
        confirm_timeout_s: float = 30.0,
        poll_interval_s: float = 1.0,
    ) -> None:
        if not 0 <= slippage_bps < 10_000:
            raise ValueError("slippage_bps must be between 0 and 9999")
        if jito_tip_lamports <= 0:
            raise ValueError("jito_tip_lamports must be positive")
        self._rpc_url = rpc_url or os.environ.get("PRIMARY_RPC_URL") or DEFAULT_RPC_URL
        self._keypair = keypair or self._load_keypair()
        self._client = http_client or httpx.AsyncClient(timeout=15.0)
        self._owns_client = http_client is None
        self._jito_client = jito_client or JitoBlockEngineClient(http_client=self._client)
        self._owns_jito_client = jito_client is None
        self._slippage_bps = slippage_bps
        self._jito_tip_lamports = jito_tip_lamports
        self._use_jito = use_jito
        self._confirm_timeout_s = confirm_timeout_s
        self._poll_interval_s = poll_interval_s
        self._closed = False

    @property
    def mode(self) -> str:
        # Direct fills share the live position lifecycle and safeguards.
        return "live"

    @property
    def wallet_pubkey(self) -> Pubkey:
        return self._keypair.pubkey()

    async def buy(
        self,
        mint_address: str,
        amount_sol: float,
        slippage_bps: int | None = None,
    ) -> TradeResult:
        """Buy a token directly from an incomplete SOL-paired Pump curve."""

        self._ensure_open()
        resolved_slippage_bps = self._resolve_slippage_bps(slippage_bps)
        amount_lamports = _sol_to_lamports(amount_sol)
        mint = Pubkey.from_string(mint_address)
        try:
            account = await fetch_bonding_curve_state(self._rpc_url, mint, http_client=self._client)
        except httpx.HTTPError as exc:
            raise RuntimeError(sanitize_provider_error(exc)) from None
        self._ensure_tradeable_curve(account.state.complete, account.state.is_sol_paired)

        expected_tokens = calculate_buy_amount(
            amount_lamports,
            account.state.virtual_sol_reserves,
            account.state.virtual_token_reserves,
        )
        token_amount = minimum_output(expected_tokens, resolved_slippage_bps)
        max_sol_cost = maximum_input(amount_lamports, resolved_slippage_bps)
        pre_token_balance = await self._token_balance_raw(mint_address)
        instructions = build_buy_instructions(
            mint=mint,
            token_program=account.token_program,
            curve=account.state,
            user=self.wallet_pubkey,
            amount=token_amount,
            max_sol_cost=max_sol_cost,
        )
        signature, status, slot, used_jito, transaction_size = await self._submit_and_confirm(
            instructions,
        )
        fill = await self._get_transaction_fill(signature, mint_address)
        actual_tokens = fill.token_delta if fill and fill.token_delta > 0 else None
        if actual_tokens is None:
            post_token_balance = await self._token_balance_raw(mint_address)
            if pre_token_balance is not None and post_token_balance is not None:
                actual_tokens = max(0, post_token_balance - pre_token_balance)
        raw_tokens = actual_tokens or token_amount
        decimals = account.token_decimals
        token_units = raw_tokens / 10**decimals
        price_sol = amount_sol / token_units if token_units else None
        return Trade(
            mint_address=mint_address,
            side=Side.BUY,
            amount_sol=amount_sol,
            token_amount=token_units,
            price_sol=price_sol,
            slippage_bps=resolved_slippage_bps,
            tx_signature=signature,
            mode=self.mode,
            status=status,
            metadata={
                "provider": "pumpfun_direct_v2",
                "expected_tokens_raw": expected_tokens,
                "minimum_tokens_raw": token_amount,
                "max_sol_cost_lamports": max_sol_cost,
                "actual_tokens_raw": actual_tokens,
                "jito": used_jito,
                "jito_tip_lamports": self._jito_tip_lamports if used_jito else 0,
                "transaction_size_bytes": transaction_size,
                "slot": slot,
            },
        )

    async def sell(
        self,
        mint_address: str,
        token_amount: float,
        slippage_bps: int | None = None,
    ) -> TradeResult:
        """Sell tokens directly into an incomplete SOL-paired Pump curve."""

        self._ensure_open()
        resolved_slippage_bps = self._resolve_slippage_bps(slippage_bps)
        mint = Pubkey.from_string(mint_address)
        try:
            account = await fetch_bonding_curve_state(self._rpc_url, mint, http_client=self._client)
        except httpx.HTTPError as exc:
            raise RuntimeError(sanitize_provider_error(exc)) from None
        self._ensure_tradeable_curve(account.state.complete, account.state.is_sol_paired)
        raw_tokens = int(token_amount * 10**account.token_decimals)
        if raw_tokens <= 0:
            raise ValueError("token_amount must produce at least one base unit")

        pre_sol_balance = await self._sol_balance_raw()
        expected_sol = calculate_sell_amount(
            raw_tokens,
            account.state.virtual_sol_reserves,
            account.state.virtual_token_reserves,
        )
        min_sol_output = minimum_output(expected_sol, resolved_slippage_bps)
        instruction = build_sell_instruction(
            mint=mint,
            token_program=account.token_program,
            curve=account.state,
            user=self.wallet_pubkey,
            amount=raw_tokens,
            min_sol_output=min_sol_output,
        )
        signature, status, slot, used_jito, transaction_size = await self._submit_and_confirm(
            [instruction],
        )
        fill = await self._get_transaction_fill(signature, mint_address)
        actual_sol = fill.sol_delta if fill and fill.sol_delta > 0 else None
        if actual_sol is None and pre_sol_balance is not None:
            post_sol_balance = await self._sol_balance_raw()
            if post_sol_balance is not None and post_sol_balance > pre_sol_balance:
                actual_sol = post_sol_balance - pre_sol_balance
        fill_reconciled = actual_sol is not None
        sol_lamports = actual_sol or 0
        sol_amount = sol_lamports / LAMPORTS_PER_SOL
        return Trade(
            mint_address=mint_address,
            side=Side.SELL,
            amount_sol=sol_amount,
            token_amount=token_amount,
            price_sol=sol_amount / token_amount if token_amount else None,
            slippage_bps=resolved_slippage_bps,
            tx_signature=signature,
            mode=self.mode,
            status=status if fill_reconciled else "confirmed_unpriced",
            metadata={
                "provider": "pumpfun_direct_v2",
                "expected_sol_lamports": expected_sol,
                "minimum_sol_output_lamports": min_sol_output,
                "actual_sol_lamports": actual_sol,
                "fill_reconciled": fill_reconciled,
                "jito": used_jito,
                "jito_tip_lamports": self._jito_tip_lamports if used_jito else 0,
                "transaction_size_bytes": transaction_size,
                "slot": slot,
            },
        )

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        """Fulfil the common adapter contract; sells interpret ``amount_sol`` as tokens."""

        if side == Side.BUY:
            return await self.buy(mint_address, amount_sol, slippage_bps)
        return await self.sell(mint_address, amount_sol, slippage_bps)

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        """Return a local constant-product quote without submitting a transaction."""

        mint = Pubkey.from_string(mint_address)
        account = await fetch_bonding_curve_state(self._rpc_url, mint, http_client=self._client)
        self._ensure_tradeable_curve(account.state.complete, account.state.is_sol_paired)
        if side == Side.BUY:
            raw_input = _sol_to_lamports(amount_sol)
            raw_output = calculate_buy_amount(
                raw_input,
                account.state.virtual_sol_reserves,
                account.state.virtual_token_reserves,
            )
            output = raw_output / 10**account.token_decimals
            price = amount_sol / output if output else None
        else:
            raw_input = int(amount_sol * 10**account.token_decimals)
            raw_output = calculate_sell_amount(
                raw_input,
                account.state.virtual_sol_reserves,
                account.state.virtual_token_reserves,
            )
            output = raw_output / LAMPORTS_PER_SOL
            price = output / amount_sol if amount_sol else None
        return SwapQuote(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            estimated_out_amount=output,
            price_sol=price,
            slippage_bps=slippage_bps,
            provider="pumpfun_direct",
            expires_at=datetime.now(UTC) + timedelta(seconds=15),
        )

    async def get_current_price(self, mint_address: str) -> float | None:
        quote = await self.get_quote(mint_address, Side.BUY, 1_000_000 / LAMPORTS_PER_SOL)
        return quote.price_sol

    async def has_active_curve(self, mint_address: str) -> bool:
        """Return whether the mint can safely use the direct Pump path now."""

        try:
            account = await fetch_bonding_curve_state(
                self._rpc_url,
                Pubkey.from_string(mint_address),
                http_client=self._client,
            )
            self._ensure_tradeable_curve(account.state.complete, account.state.is_sol_paired)
        except (CurveCompleteError, ValueError, httpx.HTTPError):
            return False
        return True

    async def close(self) -> None:
        if self._closed:
            return
        if self._owns_jito_client:
            await self._jito_client.close()
        if self._owns_client:
            await self._client.aclose()
        self._closed = True

    async def _submit_and_confirm(self, instructions) -> tuple[str, str, int | None, bool, int]:
        blockhash = await self._latest_blockhash()
        transaction = Transaction.new_signed_with_payer(
            instructions,
            self.wallet_pubkey,
            [self._keypair],
            blockhash,
        )
        serialized = bytes(transaction)
        transaction_size = len(serialized)
        log.info("DIRECT TX size=%d bytes", transaction_size)
        if transaction_size >= MAX_TRANSACTION_SIZE_BYTES:
            raise RuntimeError(
                f"direct Pump transaction is {transaction_size} bytes; maximum is "
                f"{MAX_TRANSACTION_SIZE_BYTES}",
            )
        signature = str(Signature.from_bytes(serialized[:64]))
        used_jito = await self._submit_jito(transaction, serialized)
        if not used_jito:
            signature = await self._send_rpc(serialized)
        status, slot = await self._confirm(signature)
        if status != "confirmed":
            raise RuntimeError(f"Pump direct transaction {signature} {status}")
        return signature, status, slot, used_jito, transaction_size

    async def _submit_jito(self, transaction: Transaction, serialized: bytes) -> bool:
        if not self._use_jito:
            return False
        try:
            tip = Transaction.new_signed_with_payer(
                [
                    transfer(
                        TransferParams(
                            from_pubkey=self.wallet_pubkey,
                            to_pubkey=Pubkey.from_string(random.choice(JITO_TIP_ACCOUNTS)),
                            lamports=self._jito_tip_lamports,
                        ),
                    ),
                ],
                self.wallet_pubkey,
                [self._keypair],
                transaction.message.recent_blockhash,
            )
            result = await self._jito_client._submit_bundle_for_guarded_adapter(
                [serialized, bytes(tip)],
                tip_lamports=self._jito_tip_lamports,
            )
        except Exception:
            return False
        return result.ok

    async def _latest_blockhash(self) -> Hash:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": []}
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(sanitize_provider_error(exc)) from None
        try:
            return Hash.from_string(response.json()["result"]["value"]["blockhash"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("RPC returned no usable recent blockhash") from exc

    async def _send_rpc(self, serialized: bytes) -> str:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                base64.b64encode(serialized).decode("ascii"),
                {"encoding": "base64", "skipPreflight": False, "preflightCommitment": "confirmed"},
            ],
        }
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(sanitize_provider_error(exc)) from None
        result = response.json().get("result")
        if not isinstance(result, str):
            raise RuntimeError("RPC sendTransaction returned no signature")
        return result

    async def _confirm(self, signature: str) -> tuple[str, int | None]:
        deadline = time.monotonic() + self._confirm_timeout_s
        while time.monotonic() < deadline:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignatureStatuses",
                "params": [[signature], {"searchTransactionHistory": True}],
            }
            try:
                response = await self._client.post(self._rpc_url, json=payload)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise RuntimeError(sanitize_provider_error(exc)) from None
            values = response.json().get("result", {}).get("value", [])
            status = values[0] if values else None
            if status is not None:
                if status.get("err") is not None:
                    return "failed", None
                if status.get("confirmationStatus") in ("confirmed", "finalized"):
                    return "confirmed", status.get("slot")
            await asyncio.sleep(self._poll_interval_s)
        return "expired", None

    async def _token_balance_raw(self, mint: str) -> int | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [str(self.wallet_pubkey), {"mint": mint}, {"encoding": "jsonParsed"}],
        }
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
            accounts = response.json()["result"]["value"]
            return sum(
                int(item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
                for item in accounts
            )
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            return None

    async def _sol_balance_raw(self) -> int | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [str(self.wallet_pubkey)],
        }
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
            return int(response.json()["result"]["value"])
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            return None

    async def _get_transaction_fill(self, signature: str, mint: str) -> _TransactionFill | None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        }
        try:
            response = await self._client.post(self._rpc_url, json=payload)
            response.raise_for_status()
            meta = response.json()["result"]["meta"]
            token_delta = _token_delta(meta, mint, str(self.wallet_pubkey))
            sol_delta = (
                int(meta["postBalances"][0]) - int(meta["preBalances"][0]) + int(meta["fee"])
            )
            return _TransactionFill(token_delta=token_delta, sol_delta=sol_delta)
        except (httpx.HTTPError, KeyError, TypeError, ValueError, IndexError):
            return None

    @staticmethod
    def _load_keypair() -> Keypair:
        secret = os.environ.get("WALLET_PRIVATE_KEY")
        if not secret:
            raise RuntimeError("WALLET_PRIVATE_KEY is missing from the environment")
        try:
            raw = base64.b64decode(secret)
        except Exception as exc:
            raise RuntimeError("WALLET_PRIVATE_KEY is not valid base64") from exc
        if len(raw) != 64:
            raise RuntimeError("WALLET_PRIVATE_KEY must decode to 64 bytes")
        return Keypair.from_bytes(raw)

    @staticmethod
    def _ensure_tradeable_curve(complete: bool, is_sol_paired: bool) -> None:
        if complete:
            raise CurveCompleteError("Pump bonding curve is complete; route via PumpSwap/Jupiter")
        if not is_sol_paired:
            raise ValueError("direct executor currently supports SOL-paired Pump curves only")

    def _resolve_slippage_bps(self, slippage_bps: int | None) -> int:
        resolved = self._slippage_bps if slippage_bps is None else slippage_bps
        if not 0 <= resolved < 10_000:
            raise ValueError("slippage_bps must be between 0 and 9999")
        return resolved

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("direct executor is closed")


class _TransactionFill:
    def __init__(self, token_delta: int, sol_delta: int) -> None:
        self.token_delta = token_delta
        self.sol_delta = sol_delta


def _token_delta(meta: dict, mint: str, owner: str) -> int:
    def balances(key: str) -> int:
        return sum(
            int(item["uiTokenAmount"]["amount"])
            for item in meta.get(key, [])
            if item.get("mint") == mint and item.get("owner") == owner
        )

    return balances("postTokenBalances") - balances("preTokenBalances")


def _sol_to_lamports(amount_sol: float) -> int:
    lamports = int(amount_sol * LAMPORTS_PER_SOL)
    if lamports <= 0:
        raise ValueError("amount_sol must be positive")
    return lamports
