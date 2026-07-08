import asyncio

from src.risk.holders import analyze_holder_accounts, filtered_holder_accounts, is_non_person_holder
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


def test_is_non_person_holder_excludes_known_burn_address() -> None:
    account = {
        "address": "1nc1nerator11111111111111111111111111111111",
        "uiAmount": 20.0,
    }

    assert is_non_person_holder(account) is True


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
