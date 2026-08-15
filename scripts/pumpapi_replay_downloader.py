"""Resumable PumpApi historical replay downloader for a Windows NSSM service."""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import os
import random
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import aiohttp

ARCHIVE_ROOT = Path(r"D:\pumpapi-replay")
ARCHIVE_BASE_URL = "https://replay.pumpapi.io"
ARCHIVE_START = datetime(2026, 4, 18, tzinfo=UTC)
DOWNLOAD_CONCURRENCY = 4
MANIFEST_CONCURRENCY = 1
MANIFEST_REQUEST_DELAY_SECONDS = 0.25
POLL_INTERVAL_SECONDS = 60 * 60
MAX_RETRIES = 5
CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class ArchiveHour:
    timestamp: datetime

    @property
    def key(self) -> str:
        return self.timestamp.strftime("%Y/%m/%d/%H")

    @property
    def url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.key}.jsonl.zst"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ARCHIVE_ROOT)
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument("--concurrency", type=int, default=DOWNLOAD_CONCURRENCY)
    parser.add_argument("--manifest-concurrency", type=int, default=MANIFEST_CONCURRENCY)
    parser.add_argument("--poll-seconds", type=int, default=POLL_INTERVAL_SECONDS)
    return parser.parse_args()


def configure_logging(root: Path) -> logging.Logger:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pumpapi_replay")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler = RotatingFileHandler(
        log_dir / "downloader.log", maxBytes=50_000_000, backupCount=5, encoding="utf-8"
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


def load_completed(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        completed = payload.get("completed", [])
        if not isinstance(completed, list) or not all(isinstance(key, str) for key in completed):
            raise ValueError("completed must be a list of strings")
        return set(completed)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot safely load download state {state_path}: {exc}") from exc


def save_completed(state_path: Path, completed: set[str]) -> None:
    atomic_write_json(
        state_path,
        {"completed": sorted(completed), "updated_at": datetime.now(UTC).isoformat()},
    )


def iter_hours() -> list[ArchiveHour]:
    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    timestamp = ARCHIVE_START
    hours: list[ArchiveHour] = []
    while timestamp <= current_hour:
        hours.append(ArchiveHour(timestamp))
        timestamp += timedelta(hours=1)
    return hours


async def head_entry(
    session: aiohttp.ClientSession, archive_hour: ArchiveHour, semaphore: asyncio.Semaphore
) -> dict[str, Any]:
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                # Archive availability checks are deliberately paced during startup and polling.
                await asyncio.sleep(MANIFEST_REQUEST_DELAY_SECONDS)
                async with session.head(archive_hour.url, allow_redirects=True) as response:
                    if response.status == 200:
                        content_length = response.headers.get("Content-Length")
                        if content_length is None or not content_length.isdigit():
                            return {
                                "key": archive_hour.key,
                                "url": archive_hour.url,
                                "status": "error",
                                "error": "missing_or_invalid_content_length",
                            }
                        return {
                            "key": archive_hour.key,
                            "url": archive_hour.url,
                            "status": "available",
                            "size": int(content_length),
                        }
                    if response.status == 404:
                        return {
                            "key": archive_hour.key,
                            "url": archive_hour.url,
                            "status": "missing",
                        }
                    if response.status == 429 or response.status >= 500:
                        retry_after = response.headers.get("Retry-After")
                        delay = (
                            float(retry_after)
                            if retry_after and retry_after.isdigit()
                            else min(120, 2**attempt + random.random())
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
    return {
        "key": archive_hour.key,
        "url": archive_hour.url,
        "status": "error",
        "error": "retry_exhausted",
    }


async def build_manifest(
    session: aiohttp.ClientSession, root: Path, concurrency: int, logger: logging.Logger
) -> dict[str, Any]:
    hours = iter_hours()
    logger.info("Building manifest for %d hourly archives", len(hours))
    semaphore = asyncio.Semaphore(concurrency)
    entries = await asyncio.gather(*(head_entry(session, hour, semaphore) for hour in hours))
    available = [entry for entry in entries if entry["status"] == "available"]
    missing = [entry for entry in entries if entry["status"] == "missing"]
    errors = [entry for entry in entries if entry["status"] == "error"]
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "archive_start": ARCHIVE_START.isoformat(),
        "archive_end": hours[-1].timestamp.isoformat(),
        "entries": entries,
    }
    atomic_write_json(root / "manifest.json", manifest)
    total_bytes = sum(entry["size"] for entry in available)
    logger.info(
        "Manifest saved: %d available, %d missing, %d errors, %.2f TB expected",
        len(available),
        len(missing),
        len(errors),
        total_bytes / 1_000_000_000_000,
    )
    return manifest


def expected_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [entry for entry in manifest["entries"] if entry["status"] == "available"]


def file_path(root: Path, entry: dict[str, Any]) -> Path:
    return root / "raw" / f"{entry['key']}.jsonl.zst"


def ensure_free_space(root: Path, entries: list[dict[str, Any]], completed: set[str]) -> None:
    pending_bytes = sum(entry["size"] for entry in entries if entry["key"] not in completed)
    free_bytes = shutil.disk_usage(root).free
    if pending_bytes > free_bytes:
        raise RuntimeError(
            f"Insufficient free disk space: need {pending_bytes / 1e12:.2f} TB for pending files, "
            f"have {free_bytes / 1e12:.2f} TB"
        )


async def download_entry(
    session: aiohttp.ClientSession, root: Path, entry: dict[str, Any], logger: logging.Logger
) -> int:
    destination = file_path(root, entry)
    temporary = destination.with_suffix(f"{destination.suffix}.part")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size == entry["size"]:
        return entry["size"]
    temporary.unlink(missing_ok=True)
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(entry["url"]) as response:
                if response.status == 429 or response.status >= 500:
                    await asyncio.sleep(min(120, 2**attempt + random.random()))
                    continue
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                with temporary.open("wb") as output:
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        output.write(chunk)
            actual_size = temporary.stat().st_size
            if actual_size != entry["size"]:
                raise RuntimeError(f"size mismatch: expected {entry['size']}, got {actual_size}")
            os.replace(temporary, destination)
            return actual_size
        except (TimeoutError, OSError, RuntimeError, aiohttp.ClientError) as exc:
            temporary.unlink(missing_ok=True)
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(
                    f"{entry['key']} failed after {MAX_RETRIES} attempts: {exc}"
                ) from exc
            delay = min(120, 2**attempt + random.random())
            logger.warning("Retrying %s after %s in %.1fs", entry["key"], exc, delay)
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


async def download_manifest(
    session: aiohttp.ClientSession,
    root: Path,
    manifest: dict[str, Any],
    concurrency: int,
    stopping: asyncio.Event,
    logger: logging.Logger,
) -> None:
    state_path = root / "download_state.json"
    completed = load_completed(state_path)
    entries = expected_entries(manifest)
    for entry in entries:
        destination = file_path(root, entry)
        if destination.exists() and destination.stat().st_size == entry["size"]:
            completed.add(entry["key"])
    save_completed(state_path, completed)
    ensure_free_space(root, entries, completed)
    pending = [entry for entry in entries if entry["key"] not in completed]
    total_bytes = sum(entry["size"] for entry in entries)
    completed_bytes = sum(entry["size"] for entry in entries if entry["key"] in completed)
    started = time.monotonic()
    logger.info("Starting %d pending downloads with concurrency %d", len(pending), concurrency)

    for index in range(0, len(pending), concurrency):
        if stopping.is_set():
            logger.info("Shutdown requested; current batch completed and state saved")
            break
        batch = pending[index : index + concurrency]
        results = await asyncio.gather(
            *(download_entry(session, root, entry, logger) for entry in batch),
            return_exceptions=True,
        )
        for entry, result in zip(batch, results, strict=True):
            if isinstance(result, Exception):
                logger.error("Download failed: %s", result)
                continue
            completed.add(entry["key"])
            completed_bytes += result
            save_completed(state_path, completed)
            elapsed = max(time.monotonic() - started, 0.001)
            rate = max(completed_bytes / elapsed, 1)
            remaining = max(total_bytes - completed_bytes, 0) / rate
            logger.info(
                "[%d/%d] %s - %.2f MB - %.1f%% complete - est %.1fh remaining",
                len(completed),
                len(entries),
                entry["key"],
                result / 1_000_000,
                completed_bytes / total_bytes * 100 if total_bytes else 100,
                remaining / 3600,
            )


def validate_sample(root: Path, entries: list[dict[str, Any]], logger: logging.Logger) -> None:
    if not entries:
        logger.warning("No available files to validate")
        return
    try:
        import zstandard
    except ImportError:
        logger.warning("zstandard is unavailable; sample validation skipped")
        return
    candidates = [entry for entry in entries if file_path(root, entry).exists()]
    if not candidates:
        logger.warning("No downloaded files to validate")
        return
    entry = random.choice(candidates)
    line_count = 0
    with file_path(root, entry).open("rb") as compressed:
        with zstandard.ZstdDecompressor().stream_reader(compressed) as reader:
            with io.TextIOWrapper(reader, encoding="utf-8") as text:
                for line in text:
                    json.loads(line)
                    line_count += 1
    logger.info("Validated sample %s: %d JSON events", entry["key"], line_count)


async def run(args: argparse.Namespace) -> None:
    if args.concurrency < 1 or args.manifest_concurrency < 1:
        raise ValueError("concurrency values must be positive")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    logger = configure_logging(root)
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stopping.set)
        except NotImplementedError:
            signal.signal(signum, lambda _signum, _frame: stopping.set())

    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
    connector = aiohttp.TCPConnector(limit=max(args.concurrency, args.manifest_concurrency))
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while not stopping.is_set():
            manifest = await build_manifest(session, root, args.manifest_concurrency, logger)
            if args.manifest_only:
                return
            entries = expected_entries(manifest)
            await download_manifest(session, root, manifest, args.concurrency, stopping, logger)
            completed = load_completed(root / "download_state.json")
            if all(entry["key"] in completed for entry in entries):
                validate_sample(root, entries, logger)
                total_bytes = sum(entry["size"] for entry in entries)
                logger.info(
                    "Download complete - %d hours, %.2f TB total",
                    len(entries),
                    total_bytes / 1_000_000_000_000,
                )
            if stopping.is_set():
                break
            logger.info("Polling again in %d seconds", args.poll_seconds)
            try:
                await asyncio.wait_for(stopping.wait(), timeout=args.poll_seconds)
            except TimeoutError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.exception("PumpApi replay downloader stopped: %s", exc)
        raise SystemExit(1) from exc
