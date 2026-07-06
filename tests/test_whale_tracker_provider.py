import asyncio
from pathlib import Path

import httpx

from src.core.models import SignalSource as SignalSourceEnum, SignalType
from src.signals.whale_tracker import WhaleWalletTracker


def test_whale_tracker_loads_api_key_from_dotenv(tmp_path: Path) -> None:
    config_path = tmp_path / "wallets.yaml"
    dotenv_path = tmp_path / ".env"
    config_path.write_text(
        """
wallets:
  - address: "wallet-1"
    label: "sample"
    tier: "S"
    enabled: true
""".strip(),
        encoding="utf-8",
    )
    dotenv_path.write_text("HELIUS_API_KEY=test-helius-key\nEXECUTION_MODE=paper\n", encoding="utf-8")

    tracker = WhaleWalletTracker(wallets_config_path=config_path, dotenv_path=dotenv_path)

    assert tracker._api_key == "test-helius-key"


def test_whale_tracker_provider_fetch_uses_token_account_polling(tmp_path: Path) -> None:
    config_path = tmp_path / "wallets.yaml"
    config_path.write_text(
        """
wallets:
  - address: "wallet-1"
    label: "sample"
    tier: "S"
    enabled: true
""".strip(),
        encoding="utf-8",
    )

    class RecordingClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        async def get(self, url: str, params: dict[str, object]):
            self.calls.append((url, params))

            class Response:
                def raise_for_status(self) -> None:
                    return None

                def json(self) -> object:
                    return []

            return Response()

    async def run() -> RecordingClient:
        tracker = WhaleWalletTracker(wallets_config_path=config_path, api_key="test-key")
        tracker._client = RecordingClient()
        await tracker._fetch_transactions("wallet-1")
        return tracker._client

    client = asyncio.run(run())

    assert len(client.calls) == 1
    url, params = client.calls[0]
    assert url.endswith("/v0/addresses/wallet-1/transactions")
    assert params["api-key"] == "test-key"
    assert params["limit"] == 25
    assert params["token-accounts"] == "balanceChanged"


def test_whale_tracker_normalizes_helius_buy_and_deduplicates(tmp_path: Path) -> None:
    config_path = tmp_path / "wallets.yaml"
    config_path.write_text(
        """
wallets:
  - address: "wallet-1"
    label: "sample"
    tier: "S"
    enabled: true
""".strip(),
        encoding="utf-8",
    )

    class StubWhaleWalletTracker(WhaleWalletTracker):
        async def _fetch_transactions(self, wallet_address: str) -> list[dict[str, object]]:
            assert wallet_address == "wallet-1"
            return [
                {
                    "signature": "sig-1",
                    "type": "SWAP",
                    "tokenTransfers": [
                        {
                            "mint": "mint-1",
                            "toUserAccount": {"owner": "wallet-1"},
                            "fromUserAccount": "market-maker",
                            "tokenAmount": {"uiAmount": 12345.67},
                        }
                    ],
                }
            ]

    async def run() -> tuple[list, list]:
        tracker = StubWhaleWalletTracker(wallets_config_path=config_path, api_key="test-key")
        await tracker.start()
        try:
            first_batch = await tracker.poll()
            second_batch = await tracker.poll()
        finally:
            await tracker.stop()
        return first_batch, second_batch

    first_batch, second_batch = asyncio.run(run())

    assert len(first_batch) == 1
    assert second_batch == []
    signal = first_batch[0]
    assert signal.source == SignalSourceEnum.WHALE_TRACKER
    assert signal.type == SignalType.BUY
    assert signal.mint_address == "mint-1"
    assert signal.payload["heuristics"]["is_new_position"] is True
    assert signal.payload["heuristics"]["token_amount"] == 12345.67


def test_whale_tracker_handles_empty_malformed_and_missing_key_safely(tmp_path: Path) -> None:
    config_path = tmp_path / "wallets.yaml"
    config_path.write_text(
        """
wallets:
  - address: "wallet-1"
    label: "sample"
    tier: "S"
    enabled: true
""".strip(),
        encoding="utf-8",
    )

    class EmptyTracker(WhaleWalletTracker):
        async def _fetch_transactions(self, wallet_address: str) -> list[dict[str, object]]:
            assert wallet_address == "wallet-1"
            return []

    class MalformedTracker(WhaleWalletTracker):
        async def _fetch_transactions(self, wallet_address: str) -> list[dict[str, object]]:
            assert wallet_address == "wallet-1"
            return [{"signature": "sig-1", "tokenTransfers": "not-a-list"}]

    async def run() -> tuple[list, list, list]:
        missing_key_tracker = WhaleWalletTracker(wallets_config_path=config_path, api_key="")
        empty_tracker = EmptyTracker(wallets_config_path=config_path, api_key="test-key")
        malformed_tracker = MalformedTracker(wallets_config_path=config_path, api_key="test-key")

        await empty_tracker.start()
        await malformed_tracker.start()
        try:
            missing = await missing_key_tracker.poll()
            empty = await empty_tracker.poll()
            malformed = await malformed_tracker.poll()
        finally:
            await empty_tracker.stop()
            await malformed_tracker.stop()
        return missing, empty, malformed

    missing, empty, malformed = asyncio.run(run())

    assert missing == []
    assert empty == []
    assert malformed == []
