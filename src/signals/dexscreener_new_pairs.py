"""Explicit DexScreener New Pairs UI-capture loader for paper-only workflows."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from src.core.models import Signal
from src.core.models import SignalSource as SignalSourceEnum
from src.core.models import SignalType


DEXSCREENER_PAIR_URL = "https://api.dexscreener.com/latest/dex/pairs/solana/{pair_address}"
DEXSCREENER_NEW_PAIRS_UI_URL = (
    "https://dexscreener.com/new-pairs/solana/1h?rankBy=pairAge&order=asc&maxAge=1&minLiq=1000"
    "&minMarketCap=5000&maxMarketCap=100000&profile=0"
)
UI_AGE_PATTERN = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>[smh])$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class NewPairsUiRow:
    """One rendered New Pairs UI row, captured before provider enrichment."""

    pair_address: str
    ui_age: str
    symbol: str | None = None
    name: str | None = None


def load_new_pairs_ui_rows(path: str | Path) -> list[NewPairsUiRow]:
    """Load caller-captured rows without scraping or reusing browser credentials."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("New Pairs capture must be a JSON array")

    rows: list[NewPairsUiRow] = []
    seen_pairs: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        pair_address = item.get("pair_address")
        ui_age = item.get("ui_age")
        if not isinstance(pair_address, str) or not isinstance(ui_age, str):
            continue
        normalized_pair = pair_address.strip()
        normalized_age = ui_age.strip()
        if not normalized_pair or not normalized_age or normalized_pair in seen_pairs:
            continue
        if parse_ui_age_minutes(normalized_age) is None:
            continue
        seen_pairs.add(normalized_pair)
        rows.append(
            NewPairsUiRow(
                pair_address=normalized_pair,
                ui_age=normalized_age,
                symbol=_optional_text(item.get("symbol")),
                name=_optional_text(item.get("name")),
            )
        )
    return rows


async def resolve_new_pairs_ui_rows(
    rows: Sequence[NewPairsUiRow],
    *,
    max_age_minutes: float,
    client: httpx.AsyncClient | None = None,
) -> list[Signal]:
    """Resolve captured pair IDs into read-only, UI-age-provenanced signals."""

    own_client = client is None
    active_client = client or httpx.AsyncClient(timeout=10.0)
    try:
        signals: list[Signal] = []
        for row in rows:
            pair = await _fetch_pair(active_client, row.pair_address)
            if pair is None:
                continue
            signal = _signal_from_pair(row, pair, max_age_minutes=max_age_minutes)
            if signal is not None:
                signals.append(signal)
        return signals
    finally:
        if own_client:
            await active_client.aclose()


def parse_ui_age_minutes(value: str) -> float | None:
    """Parse the bounded age text rendered by the New Pairs UI."""

    match = UI_AGE_PATTERN.fullmatch(value.strip())
    if match is None:
        return None
    amount = float(match.group("value"))
    if not math.isfinite(amount) or amount < 0:
        return None
    unit = match.group("unit").lower()
    if unit == "s":
        return amount / 60.0
    if unit == "m":
        return amount
    return amount * 60.0


async def _fetch_pair(client: httpx.AsyncClient, pair_address: str) -> dict[str, object] | None:
    try:
        response = await client.get(DEXSCREENER_PAIR_URL.format(pair_address=pair_address))
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    pair = payload.get("pair")
    return pair if isinstance(pair, dict) and pair.get("chainId") == "solana" else None


def _signal_from_pair(row: NewPairsUiRow, pair: dict[str, object], *, max_age_minutes: float) -> Signal | None:
    base_token = pair.get("baseToken")
    if not isinstance(base_token, dict):
        return None
    mint_address = base_token.get("address")
    if not isinstance(mint_address, str) or not mint_address.strip():
        return None
    ui_age_minutes = parse_ui_age_minutes(row.ui_age)
    if ui_age_minutes is None:
        return None

    pair_created_at = pair.get("pairCreatedAt")
    payload = {
        "provider": "dexscreener",
        "pair_address": row.pair_address,
        "pair_created_at": pair_created_at,
        "symbol": row.symbol or _optional_text(base_token.get("symbol")),
        "name": row.name or _optional_text(base_token.get("name")),
        "ui_age": row.ui_age,
        "ui_age_minutes": ui_age_minutes,
        "ui_max_age_minutes": max_age_minutes,
        "ui_age_source": "dexscreener_new_pairs_rendered_row",
        "read_only": True,
        "wallet_actions": False,
    }
    return Signal(
        source=SignalSourceEnum.ONCHAIN,
        type=SignalType.NEW_POOL,
        mint_address=mint_address.strip(),
        confidence=1.0,
        message=f"DexScreener New Pairs UI row for {payload['symbol'] or mint_address[:8]}",
        payload=payload,
        observed_at=datetime.now(UTC),
    )


def _optional_text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
