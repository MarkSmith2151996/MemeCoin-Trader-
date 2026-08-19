"""Run a two-hour PumpPortal WebSocket versus Jupiter Tokens V2 feed benchmark.

The feeds share one asyncio event loop and SQLite database so receipt timestamps
are directly comparable. Run from the repository root:

    setsid python3 scripts/feed_benchmark.py >> /tmp/feed_benchmark.log 2>&1 &
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = REPO_ROOT / "data" / "feed_benchmark.db"
PUMPPORTAL_URL = "wss://pumpportal.fun/api/data"
JUPITER_BASE_URL = "https://api.jup.ag/tokens/v2"
JUPITER_ENDPOINTS = (
    "/recent?limit=100",
    "/toporganicscore/5m?limit=100",
    "/toptrending/5m?limit=100",
)
DEFAULT_DURATION_SECONDS = 2 * 60 * 60


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp with millisecond precision."""
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def create_schema(connection: sqlite3.Connection) -> None:
    """Create the benchmark tables without modifying any trading database."""
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS feed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            source TEXT NOT NULL,
            mint TEXT NOT NULL,
            token_name TEXT,
            symbol TEXT,
            detected_at TEXT NOT NULL,
            raw_payload TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_mint ON feed_events(mint);
        CREATE INDEX IF NOT EXISTS idx_source_mint ON feed_events(source, mint);
        CREATE INDEX IF NOT EXISTS idx_detected ON feed_events(detected_at);

        CREATE TABLE IF NOT EXISTS benchmark_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            stopped_at TEXT,
            duration_seconds INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS feed_connection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            event TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            detail TEXT,
            FOREIGN KEY(run_id) REFERENCES benchmark_runs(id)
        );
        CREATE INDEX IF NOT EXISTS idx_connection_run_time
            ON feed_connection_events(run_id, occurred_at);
        """
    )
    columns = {row[1] for row in connection.execute("PRAGMA table_info(feed_events)")}
    if "run_id" not in columns:
        connection.execute("ALTER TABLE feed_events ADD COLUMN run_id INTEGER")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_feed_run ON feed_events(run_id)")
    connection.commit()


class FeedBenchmark:
    """Owns feed state, SQLite writes, and cooperative shutdown."""

    def __init__(self, duration_seconds: int) -> None:
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(DATABASE_PATH)
        create_schema(self.connection)
        self.duration_seconds = duration_seconds
        self.stop_event = asyncio.Event()
        self.run_id = self._start_run()
        self.jupiter_seen = {
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT mint FROM feed_events WHERE source = 'jupiter' AND run_id = ?",
                (self.run_id,),
            )
        }

    def _start_run(self) -> int:
        cursor = self.connection.execute(
            "INSERT INTO benchmark_runs (started_at, duration_seconds) VALUES (?, ?)",
            (utc_now(), self.duration_seconds),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def finish_run(self) -> None:
        self.connection.execute(
            "UPDATE benchmark_runs SET stopped_at = ? WHERE id = ?",
            (utc_now(), self.run_id),
        )
        self.connection.commit()
        self.connection.close()

    def log_connection(self, event: str, detail: str = "") -> None:
        self.connection.execute(
            """
            INSERT INTO feed_connection_events (run_id, event, occurred_at, detail)
            VALUES (?, ?, ?, ?)
            """,
            (self.run_id, event, utc_now(), detail),
        )
        self.connection.commit()

    def log_token(self, source: str, token: dict[str, Any]) -> None:
        mint = token.get("mint") or token.get("id") or token.get("address")
        if not isinstance(mint, str) or not mint:
            return
        self.connection.execute(
            """
            INSERT INTO feed_events
                (run_id, source, mint, token_name, symbol, detected_at, raw_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.run_id,
                source,
                mint,
                token.get("name"),
                token.get("symbol"),
                utc_now(),
                json.dumps(token, separators=(",", ":"), default=str),
            ),
        )
        self.connection.commit()

    def heartbeat(self) -> None:
        event_counts = dict(
            self.connection.execute(
                "SELECT source, COUNT(*) FROM feed_events WHERE run_id = ? GROUP BY source",
                (self.run_id,),
            ).fetchall()
        )
        mint_counts = dict(
            self.connection.execute(
                """
                SELECT source, COUNT(DISTINCT mint) FROM feed_events
                WHERE run_id = ? GROUP BY source
                """,
                (self.run_id,),
            ).fetchall()
        )
        overlap = self.connection.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT mint FROM feed_events
                WHERE run_id = ? AND source = 'pumpportal'
                INTERSECT
                SELECT mint FROM feed_events WHERE run_id = ? AND source = 'jupiter'
            )
            """,
            (self.run_id, self.run_id),
        ).fetchone()[0]
        print(
            "HEARTBEAT "
            f"pumpportal_events={event_counts.get('pumpportal', 0)} "
            f"jupiter_events={event_counts.get('jupiter', 0)} "
            f"pumpportal_unique={mint_counts.get('pumpportal', 0)} "
            f"jupiter_unique={mint_counts.get('jupiter', 0)} overlap={overlap}",
            flush=True,
        )

    async def pumpportal_listener(self) -> None:
        """Consume births and reconnect after PumpPortal connection rebalances."""
        backoff_seconds = 1.0
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    PUMPPORTAL_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as websocket:
                    await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                    self.log_connection("connected")
                    print("PumpPortal connected and subscribed", flush=True)
                    backoff_seconds = 1.0
                    async for message in websocket:
                        try:
                            token = json.loads(message)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(token, dict):
                            self.log_token("pumpportal", token)
                        if self.stop_event.is_set():
                            return
                    if not self.stop_event.is_set():
                        raise ConnectionError("WebSocket closed by PumpPortal")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # The service intentionally drops during rebalancing.
                detail = f"{type(exc).__name__}: {exc}"
                self.log_connection("disconnected", detail)
                print(
                    f"PumpPortal disconnected: {detail}; retrying in {backoff_seconds:.0f}s",
                    flush=True,
                )
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=backoff_seconds)
                except TimeoutError:
                    pass
                backoff_seconds = min(backoff_seconds * 2, 30.0)

    async def jupiter_poller(self) -> None:
        """Poll all MT-588 discovery endpoints once per second."""
        load_dotenv(REPO_ROOT / ".env")
        api_key = os.environ.get("JUPITER_API_KEY", "")
        if not api_key:
            print(
                "WARNING JUPITER_API_KEY is missing; "
                "Jupiter feed will retry but cannot authenticate",
                flush=True,
            )
        headers = {"x-api-key": api_key}
        async with httpx.AsyncClient(headers=headers, timeout=15) as session:
            while not self.stop_event.is_set():
                cycle_started = time.monotonic()
                results = await asyncio.gather(
                    *(self._fetch_jupiter_endpoint(session, path) for path in JUPITER_ENDPOINTS),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, Exception):
                        print(f"Jupiter poll error: {type(result).__name__}: {result}", flush=True)
                        continue
                    for token in result:
                        mint = token.get("id") or token.get("mint") or token.get("address")
                        if isinstance(mint, str) and mint and mint not in self.jupiter_seen:
                            self.jupiter_seen.add(mint)
                            self.log_token("jupiter", token)
                remaining = max(0.0, 1.0 - (time.monotonic() - cycle_started))
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=remaining)
                except TimeoutError:
                    pass

    @staticmethod
    async def _fetch_jupiter_endpoint(
        session: httpx.AsyncClient, path: str
    ) -> list[dict[str, Any]]:
        response = await session.get(f"{JUPITER_BASE_URL}{path}")
        if response.status_code != 200:
            raise RuntimeError(f"{path}: HTTP {response.status_code}: {response.text[:300]}")
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError(f"{path}: expected list, received {type(payload).__name__}")
        return [token for token in payload if isinstance(token, dict)]

    async def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except TimeoutError:
                self.heartbeat()

    async def run(self) -> None:
        print(
            f"Feed benchmark run_id={self.run_id} started_at={utc_now()} "
            f"duration_seconds={self.duration_seconds} database={DATABASE_PATH}",
            flush=True,
        )
        tasks = [
            asyncio.create_task(self.pumpportal_listener()),
            asyncio.create_task(self.jupiter_poller()),
            asyncio.create_task(self.heartbeat_loop()),
        ]
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=self.duration_seconds)
        except TimeoutError:
            print("Feed benchmark duration complete", flush=True)
        finally:
            self.stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.heartbeat()


async def async_main(duration_seconds: int) -> None:
    benchmark = FeedBenchmark(duration_seconds)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, benchmark.stop_event.set)
        except NotImplementedError:
            pass
    try:
        await benchmark.run()
    finally:
        benchmark.finish_run()
    print(
        "Feed benchmark finished_at="
        f"{utc_now()}; run the report with scripts/feed_benchmark_report.py",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        help="Benchmark duration; defaults to two hours.",
    )
    args = parser.parse_args()
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be positive")
    asyncio.run(async_main(args.duration_seconds))


if __name__ == "__main__":
    main()
