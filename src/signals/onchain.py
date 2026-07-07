"""Read-only DexScreener-backed on-chain signal source."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from src.core.models import Signal
from src.core.models import SignalSource as SignalSourceEnum
from src.core.models import SignalType
from src.signals.base import SignalSource

DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint_address}"


@dataclass(slots=True)
class PairSnapshot:
    mint_address: str
    pair_address: str
    dex_id: str | None
    symbol: str | None
    volume_m5: float
    volume_h1: float
    volume_h24: float
    buys_m5: float
    sells_m5: float
    liquidity_usd: float
    holder_count: int | None
    pair_created_at: datetime | None


class OnChainMonitor(SignalSource):
    def __init__(
        self,
        *,
        profiles_url: str = DEXSCREENER_PROFILES_URL,
        token_url_template: str = DEXSCREENER_TOKEN_URL,
        timeout_s: float = 15.0,
        profile_limit: int = 20,
        dedupe_window: timedelta = timedelta(minutes=5),
    ) -> None:
        self._profiles_url = profiles_url
        self._token_url_template = token_url_template
        self._timeout_s = timeout_s
        self._profile_limit = max(profile_limit, 1)
        self._dedupe_window = dedupe_window
        self._client: httpx.AsyncClient | None = None
        self._previous_snapshots: dict[str, PairSnapshot] = {}
        self._emitted_at: dict[str, datetime] = {}

    @property
    def name(self) -> str:
        return "onchain"

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout_s)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def poll(self) -> list[Signal]:
        self._prune_emitted_mints()
        candidate_mints = await self._fetch_candidate_mints()
        if not candidate_mints:
            return []

        signals: list[Signal] = []
        for mint_address in candidate_mints:
            snapshot = await self._fetch_snapshot(mint_address)
            if snapshot is None:
                continue

            previous = self._previous_snapshots.get(mint_address)
            self._previous_snapshots[mint_address] = snapshot

            signal = self._build_signal(snapshot, previous)
            if signal is None:
                continue
            if self._was_emitted_recently(mint_address):
                continue

            self._emitted_at[mint_address] = signal.observed_at
            signals.append(signal)

        return signals

    async def _fetch_candidate_mints(self) -> list[str]:
        payload = await self._get_json(self._profiles_url)
        if not isinstance(payload, list):
            return []

        mints: list[str] = []
        seen: set[str] = set()
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            mint_address = self._extract_str(entry, "tokenAddress", "address", "mintAddress")
            if not mint_address or mint_address in seen:
                continue
            seen.add(mint_address)
            mints.append(mint_address)
            if len(mints) >= self._profile_limit:
                break
        return mints

    async def _fetch_snapshot(self, mint_address: str) -> PairSnapshot | None:
        payload = await self._get_json(self._token_url_template.format(mint_address=mint_address))
        if not isinstance(payload, dict):
            return None

        pairs = payload.get("pairs")
        if not isinstance(pairs, list):
            return None

        best_pair = self._select_best_pair(pairs)
        if best_pair is None:
            return None
        return self._snapshot_from_pair(mint_address, best_pair)

    async def _get_json(self, url: str) -> object | None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout_s)
        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError):
            return None

    def _select_best_pair(self, pairs: list[object]) -> dict[str, object] | None:
        solana_pairs = [pair for pair in pairs if isinstance(pair, dict) and pair.get("chainId") == "solana"]
        if not solana_pairs:
            return None
        return max(solana_pairs, key=lambda pair: self._extract_float(pair.get("liquidity"), "usd"))

    def _snapshot_from_pair(self, mint_address: str, pair: dict[str, object]) -> PairSnapshot:
        volume = pair.get("volume")
        txns = pair.get("txns")
        base_token = pair.get("baseToken")
        info = pair.get("info")
        return PairSnapshot(
            mint_address=mint_address,
            pair_address=self._extract_str(pair, "pairAddress") or mint_address,
            dex_id=self._extract_str(pair, "dexId"),
            symbol=self._extract_str(base_token, "symbol") if isinstance(base_token, dict) else None,
            volume_m5=self._extract_float(volume, "m5"),
            volume_h1=self._extract_float(volume, "h1"),
            volume_h24=self._extract_float(volume, "h24"),
            buys_m5=self._extract_float(txns, "m5", "buys"),
            sells_m5=self._extract_float(txns, "m5", "sells"),
            liquidity_usd=self._extract_float(pair.get("liquidity"), "usd"),
            holder_count=self._extract_int(info, "holders") if isinstance(info, dict) else None,
            pair_created_at=self._extract_timestamp(pair.get("pairCreatedAt")),
        )

    def _build_signal(self, snapshot: PairSnapshot, previous: PairSnapshot | None) -> Signal | None:
        volume_baseline = self._volume_baseline(snapshot, previous)
        volume_score = self._score_volume_spike(snapshot.volume_m5, volume_baseline)
        buy_score = self._score_buy_sell_ratio(snapshot.buys_m5, snapshot.sells_m5)
        liquidity_score = self._score_liquidity_change(
            snapshot.liquidity_usd,
            None if previous is None else previous.liquidity_usd,
        )
        holder_score = self._score_holder_growth(
            snapshot.holder_count,
            None if previous is None else previous.holder_count,
        )
        pool_score = self._score_new_pool_activity(snapshot, previous)

        scored_candidates = [
            (SignalType.VOLUME_SPIKE, volume_score, "volume spike"),
            (SignalType.BUY, buy_score, "buy momentum"),
            (SignalType.NEW_POOL, max(liquidity_score, pool_score, holder_score), "liquidity/pool activity"),
        ]
        signal_type, confidence, label = max(scored_candidates, key=lambda item: item[1])
        if confidence < 0.3:
            return None

        symbol = snapshot.symbol or snapshot.mint_address[:8]
        payload = {
            "provider": "dexscreener",
            "pair_address": snapshot.pair_address,
            "dex_id": snapshot.dex_id,
            "symbol": snapshot.symbol,
            "metrics": {
                "volume_m5": snapshot.volume_m5,
                "volume_baseline_m5": volume_baseline,
                "buys_m5": snapshot.buys_m5,
                "sells_m5": snapshot.sells_m5,
                "liquidity_usd": snapshot.liquidity_usd,
                "holder_count": snapshot.holder_count,
                "volume_score": volume_score,
                "buy_sell_score": buy_score,
                "liquidity_score": liquidity_score,
                "holder_growth_score": holder_score,
                "new_pool_score": pool_score,
            },
            "read_only": True,
            "wallet_actions": False,
        }
        return Signal(
            source=SignalSourceEnum.ONCHAIN,
            type=signal_type,
            mint_address=snapshot.mint_address,
            confidence=confidence,
            message=f"on-chain {label} for {symbol}",
            payload=payload,
        )

    def _volume_baseline(self, snapshot: PairSnapshot, previous: PairSnapshot | None) -> float:
        if previous is not None and previous.volume_m5 > 0:
            return previous.volume_m5
        if snapshot.volume_h1 > 0:
            return snapshot.volume_h1 / 12
        if snapshot.volume_h24 > 0:
            return snapshot.volume_h24 / 288
        return 0.0

    def _score_volume_spike(self, current_volume: float, baseline_volume: float) -> float:
        if current_volume <= 0 or baseline_volume <= 0:
            return 0.0
        return self._score_ratio(current_volume / baseline_volume, ((2.0, 0.3), (5.0, 0.6), (10.0, 0.9)))

    def _score_buy_sell_ratio(self, buys: float, sells: float) -> float:
        if buys <= 0:
            return 0.0
        if sells <= 0:
            return 0.9
        return self._score_ratio(buys / sells, ((1.5, 0.3), (3.0, 0.6), (5.0, 0.9)))

    def _score_liquidity_change(self, current_liquidity: float, previous_liquidity: float | None) -> float:
        if current_liquidity <= 0 or previous_liquidity is None or previous_liquidity <= 0:
            return 0.0
        delta = current_liquidity - previous_liquidity
        if delta <= 0:
            return 0.0
        relative_change = max(delta / previous_liquidity, delta / current_liquidity)
        return self._score_ratio(relative_change, ((0.10, 0.3), (0.25, 0.6), (0.50, 0.9)))

    def _score_holder_growth(self, current_holders: int | None, previous_holders: int | None) -> float:
        if current_holders is None or previous_holders is None or previous_holders <= 0:
            return 0.0
        if current_holders <= previous_holders:
            return 0.0
        return self._score_ratio(current_holders / previous_holders, ((1.10, 0.2), (1.25, 0.4), (1.50, 0.7)))

    def _score_new_pool_activity(self, snapshot: PairSnapshot, previous: PairSnapshot | None) -> float:
        if previous is not None or snapshot.pair_created_at is None:
            return 0.0
        age = datetime.now(UTC) - snapshot.pair_created_at
        if age > timedelta(minutes=30):
            return 0.0
        activity_score = max(
            self._score_volume_spike(snapshot.volume_m5, self._volume_baseline(snapshot, previous)),
            self._score_buy_sell_ratio(snapshot.buys_m5, snapshot.sells_m5),
        )
        return min(max(0.35, activity_score), 0.9)

    def _score_ratio(
        self,
        ratio: float,
        anchors: tuple[tuple[float, float], tuple[float, float], tuple[float, float]],
    ) -> float:
        if ratio <= 0:
            return 0.0

        first_ratio, first_score = anchors[0]
        if ratio < first_ratio:
            return 0.0
        for (left_ratio, left_score), (right_ratio, right_score) in zip(anchors, anchors[1:]):
            if ratio <= right_ratio:
                span = right_ratio - left_ratio
                if span <= 0:
                    return min(max(right_score, 0.0), 1.0)
                progress = (ratio - left_ratio) / span
                return min(max(left_score + progress * (right_score - left_score), 0.0), 1.0)

        terminal_ratio, terminal_score = anchors[-1]
        if ratio >= terminal_ratio:
            return min(max(terminal_score, 0.0), 1.0)
        return min(max(first_score, 0.0), 1.0)

    def _prune_emitted_mints(self) -> None:
        now = datetime.now(UTC)
        self._emitted_at = {
            mint_address: observed_at
            for mint_address, observed_at in self._emitted_at.items()
            if now - observed_at < self._dedupe_window
        }

    def _was_emitted_recently(self, mint_address: str) -> bool:
        observed_at = self._emitted_at.get(mint_address)
        if observed_at is None:
            return False
        return datetime.now(UTC) - observed_at < self._dedupe_window

    def _extract_str(self, data: object, *keys: str) -> str:
        if not isinstance(data, dict):
            return ""
        for key in keys:
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _extract_float(self, data: object, *keys: str) -> float:
        value: object = data
        if keys:
            for key in keys:
                if not isinstance(value, dict):
                    return 0.0
                value = value.get(key)
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            return 0.0
        return 0.0

    def _extract_int(self, data: object, *keys: str) -> int | None:
        value: object = data
        if keys:
            for key in keys:
                if not isinstance(value, dict):
                    return None
                value = value.get(key)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            return None
        return None

    def _extract_timestamp(self, value: object) -> datetime | None:
        try:
            if value is None:
                return None
            timestamp_ms = float(value)
        except (TypeError, ValueError):
            return None
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
