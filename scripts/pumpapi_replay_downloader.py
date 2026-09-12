"""Range-scoped, resumable PumpAPI replay archive downloader."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import shutil
import signal
import subprocess
import sys
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, AsyncIterator

import aiohttp

ARCHIVE_ROOT = Path(r"D:\pumpapi-replay")
ARCHIVE_BASE_URL = "https://replay.pumpapi.io"
ARCHIVE_START = datetime(2026, 4, 18, tzinfo=UTC)
DOWNLOAD_CONCURRENCY = 8
MANIFEST_CONCURRENCY = 8
MAX_CONCURRENCY = 16
MAX_RETRIES = 5
CHUNK_SIZE = 1024 * 1024
MIN_FREE_GB = 280
STOP_FREE_GB = 60


@dataclass(frozen=True)
class ArchiveHour:
    timestamp: datetime

    @property
    def key(self) -> str:
        return self.timestamp.strftime("%Y/%m/%d/%H")

    @property
    def url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.key}.jsonl.zst"


@dataclass
class RunStats:
    started_at: float = field(default_factory=time.monotonic)
    initial_free_bytes: int = 0
    first_byte_seconds: float | None = None
    downloaded_bytes: int = 0
    downloaded_objects: int = 0
    failed: dict[str, str] = field(default_factory=dict)
    stopped_reason: str | None = None
    used_manifest_cache: bool = False


class AdaptiveLimiter:
    """Limits active GETs and cuts the limit when the CDN signals congestion."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.active = 0
        self._condition = asyncio.Condition()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        async with self._condition:
            await self._condition.wait_for(lambda: self.active < self.limit)
            self.active += 1
        try:
            yield
        finally:
            async with self._condition:
                self.active -= 1
                self._condition.notify_all()

    async def back_off(self, logger: logging.Logger, status: int) -> None:
        async with self._condition:
            reduced = max(1, self.limit // 2)
            if reduced < self.limit:
                logger.warning("HTTP %d: reducing GET concurrency from %d to %d", status, self.limit, reduced)
                self.limit = reduced
            self._condition.notify_all()


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must be YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ARCHIVE_ROOT)
    parser.add_argument("--start", type=parse_date, help="inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--end", type=parse_date, help="inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument("--concurrency", type=int, default=DOWNLOAD_CONCURRENCY)
    parser.add_argument("--manifest-concurrency", type=int, default=MANIFEST_CONCURRENCY)
    parser.add_argument("--min-free-gb", type=float, default=MIN_FREE_GB)
    parser.add_argument("--stop-free-gb", type=float, default=STOP_FREE_GB)
    parser.add_argument("--report-path", type=Path)
    return parser.parse_args()


def configure_logging(state_dir: Path) -> logging.Logger:
    state_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pumpapi_replay")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler = RotatingFileHandler(
        state_dir / "downloader.log", maxBytes=50_000_000, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    return logger


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def iter_hours(start: datetime | None = None, end: datetime | None = None) -> list[ArchiveHour]:
    if (start is None) != (end is None):
        raise ValueError("start and end must be supplied together")
    if start is None:
        start = ARCHIVE_START
        end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    if end < start:
        raise ValueError("end must not be before start")
    hours: list[ArchiveHour] = []
    timestamp = start
    while timestamp <= end:
        hours.append(ArchiveHour(timestamp))
        timestamp += timedelta(hours=1)
    return hours


def requested_range(args: argparse.Namespace) -> tuple[datetime | None, datetime | None]:
    if (args.start is None) != (args.end is None):
        raise ValueError("--start and --end must be supplied together")
    if args.start is None:
        return None, None
    start = datetime(args.start.year, args.start.month, args.start.day, tzinfo=UTC)
    end = datetime(args.end.year, args.end.month, args.end.day, 23, tzinfo=UTC)
    if end < start:
        raise ValueError("--end must not be before --start")
    return start, end


def range_id(hours: list[ArchiveHour]) -> str:
    return f"{hours[0].timestamp:%Y-%m-%d}_{hours[-1].timestamp:%Y-%m-%d}"


def file_path(root: Path, entry: dict[str, Any]) -> Path:
    return root / "raw" / f"{entry['key']}.jsonl.zst"


def state_paths(root: Path, hours: list[ArchiveHour]) -> tuple[Path, Path]:
    state_dir = root / "raw" / ".pumpapi-replay-state"
    identifier = range_id(hours)
    return state_dir / f"manifest-{identifier}.json", state_dir / f"state-{identifier}.json"


def load_manifest(path: Path, hours: list[ArchiveHour]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    keys = [hour.key for hour in hours]
    entries = manifest.get("entries")
    if not isinstance(entries, list) or [entry.get("key") for entry in entries] != keys:
        return None
    return manifest


def load_state(path: Path) -> dict[str, Any]:
    empty = {"completed": {}, "failed": {}}
    if not path.exists():
        return empty
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        completed = state.get("completed", {})
        failed = state.get("failed", {})
        if not isinstance(completed, dict) or not isinstance(failed, dict):
            raise ValueError("invalid completion state")
        return {"completed": completed, "failed": failed}
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot safely load download state {path}: {exc}") from exc


def save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_json(
        path,
        {
            "completed": state["completed"],
            "failed": state["failed"],
            "updated_at": datetime.now(UTC).isoformat(),
        },
    )


async def head_entry(
    session: aiohttp.ClientSession, archive_hour: ArchiveHour, semaphore: asyncio.Semaphore
) -> dict[str, Any]:
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                async with session.head(archive_hour.url, allow_redirects=True) as response:
                    if response.status == 200:
                        content_length = response.headers.get("Content-Length")
                        entry: dict[str, Any] = {
                            "key": archive_hour.key,
                            "url": archive_hour.url,
                            "status": "available",
                        }
                        if content_length is not None and content_length.isdigit():
                            entry["size"] = int(content_length)
                        return entry
                    if response.status == 404:
                        return {"key": archive_hour.key, "url": archive_hour.url, "status": "missing"}
                    if response.status == 429 or response.status >= 500:
                        retry_after = response.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after and retry_after.isdigit() else min(
                            120, 2**attempt + random.random()
                        )
                        await asyncio.sleep(delay)
                        continue
                    return {
                        "key": archive_hour.key,
                        "url": archive_hour.url,
                        "status": "error",
                        "error": f"HTTP {response.status}",
                    }
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt == MAX_RETRIES - 1:
                    return {
                        "key": archive_hour.key,
                        "url": archive_hour.url,
                        "status": "error",
                        "error": type(exc).__name__,
                    }
                await asyncio.sleep(min(60, 2**attempt + random.random()))
    return {"key": archive_hour.key, "url": archive_hour.url, "status": "error", "error": "retry_exhausted"}


async def build_manifest(
    session: aiohttp.ClientSession,
    manifest_path: Path,
    hours: list[ArchiveHour],
    concurrency: int,
    logger: logging.Logger,
) -> dict[str, Any]:
    logger.info("Building range manifest for %d hourly archives", len(hours))
    semaphore = asyncio.Semaphore(concurrency)
    entries = await asyncio.gather(*(head_entry(session, hour, semaphore) for hour in hours))
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "range_start": hours[0].timestamp.isoformat(),
        "range_end": hours[-1].timestamp.isoformat(),
        "entries": entries,
    }
    atomic_write_json(manifest_path, manifest)
    available = [entry for entry in entries if entry["status"] == "available"]
    missing = [entry for entry in entries if entry["status"] == "missing"]
    errors = [entry for entry in entries if entry["status"] == "error"]
    logger.info("Manifest saved: %d available, %d missing, %d errors", len(available), len(missing), len(errors))
    return manifest


def downloadable_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """HEAD errors remain GET candidates; otherwise coverage would be silently incomplete."""
    return [entry for entry in manifest["entries"] if entry["status"] != "missing"]


def validate_zstd_first_frame(path: Path) -> None:
    """Confirm a non-empty archive starts as a zstd frame without reading raw events."""
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError("archive is empty")
    command = ["zstd", "-q", "-d", "-c", "--", str(path)]
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise RuntimeError("zstd CLI is required for archive frame validation") from exc
    try:
        assert process.stdout is not None
        if not process.stdout.read(1):
            stderr = process.stderr.read().decode("utf-8", "replace") if process.stderr else ""
            raise RuntimeError(f"invalid or empty zstd frame: {stderr.strip()}")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def completed_on_disk(root: Path, entry: dict[str, Any], state: dict[str, Any]) -> bool:
    record = state["completed"].get(entry["key"])
    if not isinstance(record, dict) or not record.get("validated"):
        return False
    size = record.get("size")
    destination = file_path(root, entry)
    return (
        isinstance(size, int)
        and size > 0
        and destination.exists()
        and destination.stat().st_size == size
    )


def register_existing(root: Path, entries: list[dict[str, Any]], state: dict[str, Any]) -> None:
    for entry in entries:
        destination = file_path(root, entry)
        expected_size = entry.get("size")
        if expected_size is None or not destination.exists() or destination.stat().st_size != expected_size:
            continue
        validate_zstd_first_frame(destination)
        state["completed"][entry["key"]] = {"size": expected_size, "validated": True}


def ensure_initial_free_space(root: Path, minimum_gb: float) -> int:
    free_bytes = shutil.disk_usage(root).free
    if free_bytes < minimum_gb * 1_000_000_000:
        raise RuntimeError(
            f"Insufficient free disk space: {free_bytes / 1e9:.1f} GB available; "
            f"{minimum_gb:.1f} GB required before download"
        )
    return free_bytes


async def download_entry(
    session: aiohttp.ClientSession,
    root: Path,
    entry: dict[str, Any],
    limiter: AdaptiveLimiter,
    stats: RunStats,
    logger: logging.Logger,
) -> int:
    destination = file_path(root, entry)
    temporary = destination.with_suffix(f"{destination.suffix}.part")
    destination.parent.mkdir(parents=True, exist_ok=True)
    expected_size = entry.get("size")
    temporary.unlink(missing_ok=True)
    for attempt in range(MAX_RETRIES):
        try:
            async with limiter.slot():
                async with session.get(entry["url"]) as response:
                    if response.status == 429 or response.status >= 500:
                        await limiter.back_off(logger, response.status)
                        await asyncio.sleep(min(120, 2**attempt + random.random()))
                        continue
                    if response.status != 200:
                        raise RuntimeError(f"HTTP {response.status}")
                    declared_size = response.content_length
                    if expected_size is not None and declared_size is not None and declared_size != expected_size:
                        raise RuntimeError(
                            f"server size changed: HEAD {expected_size}, GET {declared_size}"
                        )
                    with temporary.open("wb") as output:
                        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                            if chunk and stats.first_byte_seconds is None:
                                stats.first_byte_seconds = time.monotonic() - stats.started_at
                            output.write(chunk)
            actual_size = temporary.stat().st_size
            if expected_size is not None and actual_size != expected_size:
                raise RuntimeError(f"size mismatch: expected {expected_size}, got {actual_size}")
            if declared_size is not None and actual_size != declared_size:
                raise RuntimeError(f"size mismatch: server declared {declared_size}, got {actual_size}")
            os.replace(temporary, destination)
            await asyncio.to_thread(validate_zstd_first_frame, destination)
            return actual_size
        except (TimeoutError, OSError, RuntimeError, aiohttp.ClientError) as exc:
            temporary.unlink(missing_ok=True)
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"{entry['key']} failed after {MAX_RETRIES} attempts: {exc}") from exc
            delay = min(120, 2**attempt + random.random())
            logger.warning("Retrying %s after %s in %.1fs", entry["key"], exc, delay)
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


def incomplete_hours(entries: list[dict[str, Any]], state: dict[str, Any]) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        if entry["key"] not in state["completed"]:
            day, hour = entry["key"].rsplit("/", 1)
            missing[day].append(hour)
    return dict(missing)


async def download_day(
    session: aiohttp.ClientSession,
    root: Path,
    entries: list[dict[str, Any]],
    state: dict[str, Any],
    state_path: Path,
    limiter: AdaptiveLimiter,
    stopping: asyncio.Event,
    stats: RunStats,
    logger: logging.Logger,
) -> None:
    pending = [entry for entry in entries if not completed_on_disk(root, entry, state)]
    if not pending:
        return
    logger.info("Starting %d pending downloads for %s", len(pending), pending[0]["key"][:10])
    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    for entry in pending:
        queue.put_nowait(entry)
    lock = asyncio.Lock()

    async def worker() -> None:
        while not stopping.is_set():
            try:
                entry = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                size = await download_entry(session, root, entry, limiter, stats, logger)
            except Exception as exc:  # Per-object failure is retained for the final report.
                async with lock:
                    stats.failed[entry["key"]] = str(exc)
                    state["failed"][entry["key"]] = str(exc)
                    save_state(state_path, state)
                logger.error("Download failed: %s", exc)
            else:
                async with lock:
                    state["completed"][entry["key"]] = {"size": size, "validated": True}
                    state["failed"].pop(entry["key"], None)
                    stats.downloaded_bytes += size
                    stats.downloaded_objects += 1
                    save_state(state_path, state)
                logger.info("Downloaded %s: %.2f MB", entry["key"], size / 1_000_000)
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(min(limiter.limit, len(pending)))]
    await asyncio.gather(*workers)


def write_report(
    path: Path,
    hours: list[ArchiveHour],
    entries: list[dict[str, Any]],
    state: dict[str, Any],
    stats: RunStats,
    final_free_bytes: int,
    concurrency: int,
) -> None:
    elapsed = max(time.monotonic() - stats.started_at, 0.001)
    incomplete = incomplete_hours(entries, state)
    completed_bytes = sum(record["size"] for record in state["completed"].values())
    lines = [
        "# MT-748 PumpAPI Raw Download",
        "",
        "## Blocker Fixes",
        "- Headerless HTTP 200 responses are available; size is verified after GET when supplied.",
        "- The manifest is scoped to the requested UTC range and cached under `raw/.pumpapi-replay-state/`.",
        "- Completion state stores validated local byte sizes, so completed objects restart without a network call.",
        "- Prior startup time-to-first-byte was approximately 15 minutes (archive-wide serial HEAD pacing).",
        f"- This run time-to-first-byte: {stats.first_byte_seconds:.2f}s" if stats.first_byte_seconds is not None else "- This run did not receive a download byte.",
        "",
        "## Run",
        f"- Requested UTC hours: {hours[0].key} through {hours[-1].key} ({len(hours)} hours).",
        f"- GET concurrency started at {concurrency}; adaptive reductions are logged when the CDN returns 429/5xx.",
        f"- Objects downloaded this run: {stats.downloaded_objects}.",
        f"- Bytes downloaded this run: {stats.downloaded_bytes}.",
        f"- Total validated bytes on disk for range: {completed_bytes}.",
        f"- Wall time: {elapsed:.2f}s.",
        f"- Achieved throughput: {stats.downloaded_bytes / elapsed / 1_000_000:.2f} MB/s.",
        f"- D: free space before: {stats.initial_free_bytes / 1e9:.2f} GB; after: {final_free_bytes / 1e9:.2f} GB.",
        "",
        "## Coverage",
    ]
    if incomplete:
        for day, missing in sorted(incomplete.items()):
            lines.append(f"- Incomplete {day}: missing hours {', '.join(missing)}.")
    else:
        lines.append("- All requested days contain 24 validated hourly objects.")
    lines.extend(["", "## Failures"])
    failures = {**state["failed"], **stats.failed}
    if failures:
        for key, error in sorted(failures.items()):
            lines.append(f"- {key}: {error}")
    else:
        lines.append("- None.")
    if stats.stopped_reason:
        lines.extend(["", "## Stop Reason", f"- {stats.stopped_reason}"])
    atomic_write_text(path, "\n".join(lines) + "\n")


async def run(args: argparse.Namespace) -> int:
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        raise ValueError(f"--concurrency must be between 1 and {MAX_CONCURRENCY}")
    if not 1 <= args.manifest_concurrency <= MAX_CONCURRENCY:
        raise ValueError(f"--manifest-concurrency must be between 1 and {MAX_CONCURRENCY}")
    if args.stop_free_gb >= args.min_free_gb:
        raise ValueError("--stop-free-gb must be lower than --min-free-gb")
    start, end = requested_range(args)
    hours = iter_hours(start, end)
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest_path, state_path = state_paths(root, hours)
    logger = configure_logging(manifest_path.parent)
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stopping.set)
        except NotImplementedError:
            signal.signal(signum, lambda _signum, _frame: stopping.set())

    stats = RunStats(initial_free_bytes=ensure_initial_free_space(root, args.min_free_gb))
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
    connector = aiohttp.TCPConnector(limit=max(args.concurrency, args.manifest_concurrency))
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        manifest = load_manifest(manifest_path, hours)
        if manifest is None:
            manifest = await build_manifest(session, manifest_path, hours, args.manifest_concurrency, logger)
        else:
            stats.used_manifest_cache = True
            logger.info("Loaded cached range manifest in %.2fs", time.monotonic() - stats.started_at)
        required_entries = manifest["entries"]
        entries = downloadable_entries(manifest)
        state = load_state(state_path)
        register_existing(root, entries, state)
        save_state(state_path, state)
        if args.manifest_only:
            logger.info("Manifest ready for %d available objects", len(entries))
            return 0

        by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_day_downloadable: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entry in required_entries:
            by_day[entry["key"][:10]].append(entry)
        for entry in entries:
            by_day_downloadable[entry["key"][:10]].append(entry)
        limiter = AdaptiveLimiter(args.concurrency)
        for day, day_entries in sorted(by_day.items()):
            if stopping.is_set():
                stats.stopped_reason = "Signal received; completed objects were saved for resume."
                break
            await download_day(
                session,
                root,
                by_day_downloadable[day],
                state,
                state_path,
                limiter,
                stopping,
                stats,
                logger,
            )
            free_bytes = shutil.disk_usage(root).free
            day_complete = all(completed_on_disk(root, entry, state) for entry in day_entries)
            if day_complete:
                logger.info("Completed day %s; %.2f GB free", day, free_bytes / 1e9)
                complete_days = sum(
                    all(completed_on_disk(root, entry, state) for entry in entries_for_day)
                    for entries_for_day in by_day.values()
                )
                completed_bytes = sum(record["size"] for record in state["completed"].values())
                projected_total = completed_bytes / complete_days * len(by_day)
                if projected_total - completed_bytes > free_bytes - args.stop_free_gb * 1e9:
                    stats.stopped_reason = (
                        f"Projected remaining download ({(projected_total - completed_bytes) / 1e9:.1f} GB) "
                        f"would breach the {args.stop_free_gb:.1f} GB free-space floor."
                    )
                    logger.error(stats.stopped_reason)
                    break
            if free_bytes < args.stop_free_gb * 1e9:
                stats.stopped_reason = (
                    f"Free space fell below the {args.stop_free_gb:.1f} GB safety floor after {day}."
                )
                logger.error(stats.stopped_reason)
                break

    final_free_bytes = shutil.disk_usage(root).free
    if args.report_path:
        write_report(
            args.report_path,
            hours,
            required_entries,
            state,
            stats,
            final_free_bytes,
            args.concurrency,
        )
    incomplete = incomplete_hours(required_entries, state)
    logger.info("Finished: %d complete, %d incomplete", len(state["completed"]), sum(map(len, incomplete.values())))
    return 0 if not stats.stopped_reason and not incomplete else 2


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(run(parse_args())))
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.exception("PumpAPI replay downloader stopped: %s", exc)
        raise SystemExit(1) from exc
