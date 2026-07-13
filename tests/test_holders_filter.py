import asyncio

from src.core.config import RiskConfig
from src.core.models import CheckResult, TokenInfo
from src.risk.holders import (
    RAYDIUM_AMM_PROGRAM_ADDRESS,
    SYSTEM_PROGRAM_ADDRESS,
    analyze_holder_accounts,
    check_top10_holders,
    filtered_holder_accounts,
    is_non_person_holder,
)
from src.risk.scorer import ReadOnlyHolderLookup


class FakeRpcClient:
    def __init__(self, responses: dict[str, object]) -> None:
        self._responses = responses
        self.closed = False

    async def call(self, method: str, params: list[object] | None = None) -> object:
        return self._responses[method]

    async def close(self) -> None:
        self.closed = True


def test_is_non_person_holder_excludes_known_program_owned_account() -> None:
    account = {
        "address": "human-token-account",
        "owner": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "uiAmount": 45.0,
    }

    assert is_non_person_holder(account) is True


def test_analyze_holder_accounts_excludes_burn_and_null_addresses() -> None:
    accounts = [
        {"address": "1nc1nerator11111111111111111111111111111111", "uiAmount": 20.0},
        {"address": SYSTEM_PROGRAM_ADDRESS, "uiAmount": 15.0},
        {"address": "system-owned", "owner": SYSTEM_PROGRAM_ADDRESS, "uiAmount": 10.0},
        {"address": "holder-1", "owner": "owner-1", "uiAmount": 10.0},
    ]

    filtered, diagnostics = analyze_holder_accounts(accounts)

    assert [account["address"] for account in filtered] == ["holder-1"]
    assert [entry["classification"] for entry in diagnostics["top_filtered_accounts"]] == [
        "burn_address",
        "null_or_system_address",
        "system_program_owner",
    ]


def test_filtered_holder_accounts_keeps_normal_wallet_like_holder() -> None:
    accounts = [
        {
            "address": "wallet-token-account",
            "owner": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
            "uiAmount": 12.5,
        }
    ]

    filtered = filtered_holder_accounts(accounts)

    assert filtered == accounts


def test_analyze_holder_accounts_excludes_pumpfun_bonding_curve_address() -> None:
    accounts = [
        {
            "address": "bonding-curve-account",
            "owner": "wallet-owner",
            "uiAmount": 100.0,
        },
        {
            "address": "holder-1",
            "owner": "owner-1",
            "uiAmount": 5.0,
        },
    ]

    filtered, diagnostics = analyze_holder_accounts(
        accounts,
        extra_excluded_addresses={"bonding-curve-account"},
    )

    assert [account["address"] for account in filtered] == ["holder-1"]
    assert diagnostics["filtered_account_count"] == 1
    assert diagnostics["top_filtered_accounts"][0]["classification"] == "bonding_curve_artifact"


def test_analyze_holder_accounts_excludes_identified_raydium_pool_and_pda() -> None:
    accounts = [
        {
            "address": "raydium-pool",
            "programId": RAYDIUM_AMM_PROGRAM_ADDRESS,
            "accountType": "pool",
            "uiAmount": 40.0,
        },
        {
            "address": "raydium-pda",
            "programId": RAYDIUM_AMM_PROGRAM_ADDRESS,
            "isPda": True,
            "uiAmount": 30.0,
        },
        {"address": "holder-1", "owner": "owner-1", "uiAmount": 20.0},
    ]

    filtered, diagnostics = analyze_holder_accounts(accounts)

    assert [account["address"] for account in filtered] == ["holder-1"]
    assert [entry["classification"] for entry in diagnostics["top_filtered_accounts"]] == [
        "raydium_pool_or_pda",
        "raydium_pool_or_pda",
    ]


def test_analyze_holder_accounts_keeps_user_wallet_with_100pct_holding() -> None:
    accounts = [
        {
            "address": "user-wallet-token-account",
            "owner": "real-user-wallet",
            "uiAmount": 100.0,
        }
    ]

    filtered, diagnostics = analyze_holder_accounts(accounts)

    assert [account["address"] for account in filtered] == ["user-wallet-token-account"]
    assert diagnostics["filtered_account_count"] == 0
    assert diagnostics["top_retained_accounts"][0]["classification"] == "retained"


def test_read_only_holder_lookup_keeps_real_whale_and_applies_50pct_threshold() -> None:
    rpc_client = FakeRpcClient(
        {
            "getTokenSupply": {"value": {"uiAmount": 100.0}},
            "getTokenLargestAccounts": {
                "value": [
                    {"address": "raydium-pool", "owner": RAYDIUM_AMM_PROGRAM_ADDRESS, "uiAmount": 60.0},
                    {"address": "real-whale", "owner": "real-wallet", "uiAmount": 51.0},
                ]
            },
        }
    )
    lookup = ReadOnlyHolderLookup(
        rpc_url="https://example.invalid",
        rpc_client_factory=lambda _url, _timeout: rpc_client,
    )

    result = asyncio.run(lookup.fetch("mint"))

    assert result is not None
    assert result.status == "holder_lookup_succeeded"
    assert result.top10_holder_pct == 51.0
    assert result.filtered_account_count == 1
    assert (
        check_top10_holders(
            TokenInfo(mint_address="mint", top10_holder_pct=result.top10_holder_pct),
            RiskConfig(),
        )
        == CheckResult.FAIL
    )


def test_read_only_holder_lookup_returns_unknown_when_every_account_is_excluded() -> None:
    rpc_client = FakeRpcClient(
        {
            "getTokenSupply": {"value": {"uiAmount": 100.0}},
            "getTokenLargestAccounts": {
                "value": [
                    {"address": "1nc1nerator11111111111111111111111111111111", "uiAmount": 60.0},
                    {"address": SYSTEM_PROGRAM_ADDRESS, "uiAmount": 40.0},
                ]
            },
        }
    )
    lookup = ReadOnlyHolderLookup(
        rpc_url="https://example.invalid",
        rpc_client_factory=lambda _url, _timeout: rpc_client,
    )

    result = asyncio.run(lookup.fetch("mint"))

    assert result is not None
    assert result.status == "holder_lookup_no_largest_accounts"
    assert result.top10_holder_pct is None


def test_read_only_holder_lookup_recalculates_concentration_after_filtering() -> None:
    rpc_client = FakeRpcClient(
        {
            "getTokenSupply": {"value": {"uiAmount": 100.0}},
            "getTokenLargestAccounts": {
                "value": [
                    {
                        "address": "raydium-pool-account",
                        "owner": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
                        "uiAmount": 40.0,
                    },
                    {
                        "address": "1nc1nerator11111111111111111111111111111111",
                        "uiAmount": 15.0,
                    },
                    {
                        "address": "holder-1",
                        "owner": "owner-1",
                        "uiAmount": 20.0,
                    },
                    {
                        "address": "holder-2",
                        "owner": "owner-2",
                        "uiAmount": 10.0,
                    },
                    {
                        "address": "holder-3",
                        "owner": "owner-3",
                        "uiAmount": 5.0,
                    },
                ]
            },
        }
    )
    lookup = ReadOnlyHolderLookup(
        rpc_url="https://example.invalid",
        rpc_client_factory=lambda _url, _timeout: rpc_client,
    )

    result = asyncio.run(lookup.fetch("mint"))

    assert result is not None
    assert result.status == "holder_lookup_succeeded"
    assert result.top10_holder_pct == 35.0
    assert rpc_client.closed is True
