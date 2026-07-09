"""Aggregate signals from multiple async sources."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections import defaultdict
from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path
from typing import Any

import aiosqlite

from src.core.models import Signal
from src.signals.base import SignalSource


PROMOTIONAL_IDENTITY_TOKENS = {
    "alert",
    "ape",
    "buy",
    "follow",
    "followers",
    "join",
    "moon",
    "now",
    "pump",
    "raid",
    "read",
    "send",
}


class SignalAggregator:
    def __init__(
        self,
        sources: Iterable[SignalSource] = (),
        db: object | None = None,
        dedup_window_seconds: int = 300,
    ) -> None:
        self.sources = list(sources)
        self.db = db
        self.dedup_window = timedelta(seconds=max(dedup_window_seconds, 0))
        self._latest_opportunities: list[Signal] = []
        self._last_source_signal_counts: dict[str, int] = {}
        self._last_source_failures: dict[str, int] = {}
        self._last_raw_signal_count = 0
        self._last_composite_count = 0
        self._last_social_credibility: dict[str, object] | None = None

    async def start(self) -> None:
        await self._run_source_method("start")

    async def stop(self) -> None:
        await self._run_source_method("stop")

    async def poll(self) -> list[Signal]:
        return await self.poll_all()

    async def poll_all(self) -> list[Signal]:
        if not self.sources:
            self._latest_opportunities = []
            self._last_source_signal_counts = {}
            self._last_source_failures = {}
            self._last_raw_signal_count = 0
            self._last_composite_count = 0
            self._last_social_credibility = None
            return []

        raw_signals: list[Signal] = []
        self._last_source_signal_counts = {}
        self._last_source_failures = {}
        results = await asyncio.gather(
            *(source.poll() for source in self.sources),
            return_exceptions=True,
        )
        for source, result in zip(self.sources, results):
            if isinstance(result, Exception):
                self._last_source_failures[source.name] = self._last_source_failures.get(source.name, 0) + 1
                continue
            self._last_source_signal_counts[source.name] = len(result)
            raw_signals.extend(result)

        opportunities = self._rank_signals(raw_signals)
        await self._log_signals(opportunities)
        self._latest_opportunities = opportunities
        self._last_raw_signal_count = len(raw_signals)
        self._last_composite_count = sum(
            1
            for signal in opportunities
            if isinstance(signal.payload.get("source_count"), int) and signal.payload["source_count"] > 1
        )
        self._last_social_credibility = self._aggregate_social_credibility(raw_signals)
        return list(opportunities)

    async def get_top_opportunities(self, n: int = 10) -> list[Signal]:
        return list(self._latest_opportunities[: max(n, 0)])

    def diagnostics(self) -> dict[str, object]:
        diagnostics: dict[str, object] = {
            "sources_polled": [source.name for source in self.sources],
            "source_signal_counts": dict(sorted(self._last_source_signal_counts.items())),
            "source_failures": dict(sorted(self._last_source_failures.items())),
            "raw_signal_count": self._last_raw_signal_count,
            "composite_opportunities": self._last_composite_count,
            "ranked_opportunities": len(self._latest_opportunities),
        }
        if self._last_social_credibility is not None:
            diagnostics["social_credibility"] = self._last_social_credibility
        return diagnostics

    async def _run_source_method(self, method_name: str) -> None:
        if not self.sources:
            return

        await asyncio.gather(
            *(getattr(source, method_name)() for source in self.sources),
            return_exceptions=True,
        )

    def _rank_signals(self, signals: Iterable[Signal]) -> list[Signal]:
        grouped: dict[str, list[Signal]] = defaultdict(list)
        for signal in signals:
            grouped[signal.mint_address].append(signal)

        ranked: list[Signal] = []
        for mint_signals in grouped.values():
            ranked.extend(self._build_clusters(mint_signals))

        return sorted(ranked, key=self._composite_sort_key, reverse=True)

    def _build_clusters(self, signals: list[Signal]) -> list[Signal]:
        if not signals:
            return []

        ordered = sorted(signals, key=lambda signal: signal.observed_at)
        clusters: list[list[Signal]] = []
        current_cluster: list[Signal] = []

        for signal in ordered:
            if not current_cluster:
                current_cluster = [signal]
                continue

            previous_signal = current_cluster[-1]
            if (signal.observed_at - previous_signal.observed_at) <= self.dedup_window:
                current_cluster.append(signal)
                continue

            clusters.append(current_cluster)
            current_cluster = [signal]

        if current_cluster:
            clusters.append(current_cluster)

        return [self._decorate_cluster_signal(cluster) for cluster in clusters]

    def _decorate_cluster_signal(self, cluster: list[Signal]) -> Signal:
        signal = self._composite_signal(cluster) if len(cluster) > 1 else cluster[0]
        context = self._pump_fun_identity_context(cluster)
        if context is None:
            return signal

        payload = dict(signal.payload) if isinstance(signal.payload, dict) else {}
        payload["pump_fun_identity_context"] = context
        return signal.model_copy(update={"payload": payload})

    def _composite_signal(self, cluster: list[Signal]) -> Signal:
        if len(cluster) == 1:
            return cluster[0]

        distinct_sources = {signal.source for signal in cluster}
        multiplier = 2.0 if len(distinct_sources) >= 3 else 1.5
        base_signal = max(cluster, key=self._signal_score)
        composite_score = min(self._signal_score(base_signal) * multiplier, 1.0)
        latest_signal = max(cluster, key=lambda signal: signal.observed_at)
        payload = dict(latest_signal.payload)
        latest_social_signal = max(
            (signal for signal in cluster if signal.source.value == "TWITTER"),
            key=lambda signal: signal.observed_at,
            default=None,
        )
        if latest_social_signal is not None:
            social_payload = latest_social_signal.payload if isinstance(latest_social_signal.payload, dict) else {}
            for key in ("credibility_tier", "credibility_avg_score", "credibility_by_author"):
                if key in social_payload and key not in payload:
                    payload[key] = social_payload[key]
        social_credibility = self._aggregate_social_credibility(cluster)
        payload.update(
            {
                "raw_data": [signal.model_dump(mode="json") for signal in cluster],
                "composite_score": composite_score,
                "source_count": len(distinct_sources),
                "sources": [source.value for source in sorted(distinct_sources)],
            }
        )
        if social_credibility is not None:
            payload["social_credibility"] = social_credibility

        return latest_signal.model_copy(
            update={
                "confidence": composite_score,
                "weight": 1.0,
                "message": self._composite_message(cluster, len(distinct_sources), composite_score),
                "payload": payload,
            }
        )

    def _composite_message(self, cluster: list[Signal], source_count: int, composite_score: float) -> str:
        sources = ", ".join(source.value for source in sorted({signal.source for signal in cluster}))
        return (
            f"Composite signal from {source_count} sources ({sources}) "
            f"score={composite_score:.2f}"
        )

    def _composite_sort_key(self, signal: Signal) -> tuple[float, float]:
        penalty_points = self._ranking_penalty_points(signal)
        adjusted_score = max(self._signal_score(signal) - penalty_points, 0.0)
        return (adjusted_score, signal.observed_at.timestamp())

    def _signal_score(self, signal: Signal) -> float:
        return min(signal.confidence * signal.weight, 1.0)

    def _ranking_penalty_points(self, signal: Signal) -> float:
        payload = signal.payload if isinstance(signal.payload, dict) else {}
        context = payload.get("pump_fun_identity_context")
        if not isinstance(context, dict):
            return 0.0
        penalty_points = context.get("ranking_penalty_points")
        if isinstance(penalty_points, (int, float)):
            return max(0.0, min(float(penalty_points), 0.12))
        return 0.0

    def _pump_fun_identity_context(self, cluster: list[Signal]) -> dict[str, object] | None:
        pump_payloads = [
            signal.payload
            for signal in cluster
            if signal.source.value == "PUMP_FUN" and isinstance(signal.payload, dict)
        ]
        if not pump_payloads:
            return None

        metadata_state = "rich"
        weak_identity_name = False
        reasons: list[str] = []
        for payload in pump_payloads:
            attention_diagnostics = payload.get("attention_diagnostics")
            if isinstance(attention_diagnostics, dict):
                candidate_state = attention_diagnostics.get("metadata_completeness_state")
                if candidate_state == "sparse":
                    metadata_state = "sparse"
                elif candidate_state == "partial" and metadata_state != "sparse":
                    metadata_state = "partial"
            if self._looks_promotional_identity(payload):
                weak_identity_name = True

        if metadata_state == "partial":
            reasons.append("partial_metadata")
        elif metadata_state == "sparse":
            reasons.append("sparse_metadata")
        if weak_identity_name:
            reasons.append("weak_identity")

        penalty_points = 0.0
        if metadata_state == "partial":
            penalty_points += 0.03
        elif metadata_state == "sparse":
            penalty_points += 0.06
        if weak_identity_name:
            penalty_points += 0.06

        return {
            "has_pump_fun": True,
            "metadata_state": metadata_state,
            "weak_identity_name": weak_identity_name,
            "reasons": reasons,
            "ranking_penalty_points": round(min(penalty_points, 0.12), 6),
        }

    def _looks_promotional_identity(self, payload: dict[str, object]) -> bool:
        for field_name in ("name", "symbol", "ticker"):
            value = payload.get(field_name)
            if not isinstance(value, str) or not value.strip():
                continue
            tokens = [
                token.lower()
                for token in "".join(character if character.isalnum() else " " for character in value).split()
                if token
            ]
            if len(tokens) < 2:
                continue
            promotional_hits = sum(1 for token in tokens if token in PROMOTIONAL_IDENTITY_TOKENS)
            if promotional_hits >= 2:
                return True
        return False

    def _aggregate_social_credibility(self, signals: Iterable[Signal]) -> dict[str, object] | None:
        author_tiers: dict[str, str] = {}
        tier_counts: dict[str, int] = defaultdict(int)
        highest_tier: str | None = None
        spam_flagged_accounts = 0
        duplicate_suppression_posts = 0

        for signal in signals:
            if signal.source.value != "TWITTER":
                continue
            payload = signal.payload if isinstance(signal.payload, dict) else {}
            credibility_by_author = payload.get("credibility_by_author")
            if not isinstance(credibility_by_author, dict):
                tier = payload.get("credibility_tier")
                if isinstance(tier, str) and tier:
                    tier_counts[tier] += 1
                    highest_tier = self._higher_social_tier(highest_tier, tier)
                continue

            for author_id, details in credibility_by_author.items():
                if author_id in author_tiers or not isinstance(details, dict):
                    continue
                tier = details.get("tier") if isinstance(details.get("tier"), str) else "unknown"
                author_tiers[author_id] = tier
                tier_counts[tier] += 1
                highest_tier = self._higher_social_tier(highest_tier, tier)

                spam_flags = details.get("spam_flags")
                if isinstance(spam_flags, list) and spam_flags:
                    spam_flagged_accounts += 1

                duplicate_posts = details.get("duplicate_posts")
                if isinstance(duplicate_posts, int):
                    duplicate_suppression_posts += max(duplicate_posts, 0)

        if not tier_counts:
            return None

        return {
            "highest_tier": highest_tier or "unknown",
            "unique_accounts": len(author_tiers),
            "tier_distribution": dict(sorted(tier_counts.items())),
            "spam_flagged_accounts": spam_flagged_accounts,
            "duplicate_suppression_posts": duplicate_suppression_posts,
        }

    def _higher_social_tier(self, current: str | None, candidate: str) -> str:
        tier_rank = {"unknown": 0, "C": 1, "B": 2, "A": 3, "S": 4}
        if current is None:
            return candidate
        return candidate if tier_rank.get(candidate, -1) > tier_rank.get(current, -1) else current

    async def _log_signals(self, signals: list[Signal]) -> None:
        for signal in signals:
            try:
                await self._log_signal(signal)
            except Exception:
                continue

    async def _log_signal(self, signal: Signal) -> None:
        if self.db is None:
            return

        recorder = getattr(self.db, "record_signal", None)
        if callable(recorder):
            result = recorder(signal)
            if inspect.isawaitable(result):
                await result
            return

        if isinstance(self.db, (str, Path)):
            await self._record_signal_to_sqlite(Path(self.db), signal)

    async def _record_signal_to_sqlite(self, db_path: Path, signal: Signal) -> None:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'signals'"
            )
            table_row = await cursor.fetchone()
            await cursor.close()
            if table_row is None:
                return

            cursor = await db.execute("PRAGMA table_info(signals)")
            columns = [row[1] for row in await cursor.fetchall()]
            await cursor.close()
            if not columns:
                return

            payload_json = signal.model_dump_json()
            values_by_column: dict[str, Any] = {
                "source": signal.source.value,
                "type": signal.type.value,
                "mint_address": signal.mint_address,
                "confidence": signal.confidence,
                "weight": signal.weight,
                "message": signal.message,
                "observed_at": signal.observed_at.isoformat(),
                "created_at": signal.observed_at.isoformat(),
                "payload_json": payload_json,
                "metadata_json": payload_json,
                "payload": json.dumps(signal.payload, default=str),
            }
            insertable_columns = [column for column in columns if column in values_by_column]
            if not insertable_columns:
                return

            placeholders = ", ".join("?" for _ in insertable_columns)
            sql = f"INSERT INTO signals ({', '.join(insertable_columns)}) VALUES ({placeholders})"
            await db.execute(sql, tuple(values_by_column[column] for column in insertable_columns))
            await db.commit()
