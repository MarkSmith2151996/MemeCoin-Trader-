#!/usr/bin/env python3
"""Self-healing monitor for the paper-trading runtime.

Cron invokes this once per minute. It deliberately does not rely on the
Telegram bot: alerts are sent with a direct Telegram HTTP request so a bot
restart remains visible to the operator.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.error import URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = Path("/tmp/health_monitor.log")
LOCK_PATH = Path("/tmp/health_monitor.lock")
CHROME_CDP_URL = "http://172.21.32.1:19223/json/version"
CHROME_DEBUG_PORT = 9262
CHROME_PROFILE = r"D:\chrome-mt-profile"
BROWSER_PC_DIR = Path("/home/dev/browser_service")
BROWSER_PC_HEALTH = "http://localhost:8099/health"
BROWSER_PC_CAPTURE = "http://localhost:8099/capture"
CAPTURE_TEST_URL = "https://example.com"
BOT_CONFIG_PATH = PROJECT_ROOT / "bot/config.json"


@dataclass(frozen=True)
class CheckResult:
    ok: bool
    detail: str


class UtcFormatter(logging.Formatter):
    converter = time.gmtime


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("health_monitor")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    formatter = UtcFormatter("%(asctime)s UTC %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


class HealthMonitor:
    """Probe each runtime dependency and restart only the failed component."""

    def __init__(
        self,
        *,
        sleep: Callable[[float], None] = time.sleep,
        command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.log = configure_logging()
        self.sleep = sleep
        self.command_runner = command_runner

    def run_cycle(self) -> bool:
        """Run every check. Return True when all components were healthy."""
        healthy = True

        chrome = self.check_chrome()
        if not chrome.ok:
            healthy = False
            recovered = self.restart_chrome().ok
            self.report_failure("Chrome CDP", chrome.detail, recovered)

        browser = self.check_browser_pc()
        if not browser.ok:
            healthy = False
            recovered = self.restart_browser_pc().ok
            self.report_failure("browser-pc", browser.detail, recovered)

        for name, pattern, restart in (
            ("Strategy B", "run_strategy_b.py", self.restart_strategy_b),
            ("Telegram bot", "node bot.js", self.restart_telegram_bot),
        ):
            process = self.check_process(pattern)
            if not process.ok:
                healthy = False
                recovered = restart().ok
                self.report_failure(name, process.detail, recovered)

        if healthy:
            self.log.info("all components healthy")
        return healthy

    def check_chrome(self) -> CheckResult:
        try:
            with urlopen(CHROME_CDP_URL, timeout=3) as response:
                if response.status != 200:
                    return CheckResult(False, f"CDP returned HTTP {response.status}")
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, URLError, json.JSONDecodeError) as exc:
            return CheckResult(False, f"CDP unreachable: {type(exc).__name__}")

        if not payload.get("Browser"):
            return CheckResult(False, "CDP response lacks Browser field")
        return CheckResult(True, "CDP responding through portproxy")

    def check_browser_pc(self) -> CheckResult:
        try:
            health = self.request_json(BROWSER_PC_HEALTH, method="GET", timeout=5)
        except (OSError, URLError, json.JSONDecodeError) as exc:
            return CheckResult(False, f"health endpoint unavailable: {type(exc).__name__}")

        if health.get("status") != "ok":
            return CheckResult(False, f"health status is {health.get('status')!r}")

        try:
            request = Request(
                BROWSER_PC_CAPTURE,
                data=json.dumps({"url": CAPTURE_TEST_URL, "wait_seconds": 3}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=45) as response:
                if response.status != 200:
                    return CheckResult(False, f"capture returned HTTP {response.status}")
                response.read()
        except (OSError, URLError) as exc:
            return CheckResult(False, f"capture failed: {type(exc).__name__}")

        return CheckResult(True, "health and capture responding")

    def check_process(self, pattern: str) -> CheckResult:
        result = self.command_runner(
            ["pgrep", "-f", pattern],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return CheckResult(True, f"pid {result.stdout.splitlines()[0]}")
        return CheckResult(False, f"process not found ({pattern})")

    def restart_chrome(self) -> CheckResult:
        self.log.warning("restarting Chrome on Windows debug port %s", CHROME_DEBUG_PORT)
        command = (
            "Start-Process 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe' "
            "-ArgumentList "
            f"'--remote-debugging-port={CHROME_DEBUG_PORT}',"
            f"'--user-data-dir={CHROME_PROFILE}',"
            "'--no-first-run','--no-default-browser-check' -WindowStyle Minimized"
        )
        try:
            self.command_runner(
                ["powershell.exe", "-NoProfile", "-Command", command],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return CheckResult(False, f"Windows start failed: {type(exc).__name__}")

        self.sleep(8)
        local_port = self.check_windows_chrome_port()
        proxied_port = self.check_chrome()
        if local_port.ok and proxied_port.ok:
            return CheckResult(True, "Windows 9262 and portproxy 19223 responding")
        return CheckResult(False, f"9262={local_port.detail}; 19223={proxied_port.detail}")

    def check_windows_chrome_port(self) -> CheckResult:
        try:
            result = self.command_runner(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-Command",
                    f"Test-NetConnection -ComputerName 127.0.0.1 -Port {CHROME_DEBUG_PORT} -InformationLevel Quiet",
                ],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return CheckResult(False, type(exc).__name__)
        if result.returncode == 0 and result.stdout.strip().lower() == "true":
            return CheckResult(True, "Windows port 9262 responding")
        return CheckResult(False, "Windows port 9262 unavailable")

    def restart_browser_pc(self) -> CheckResult:
        self.log.warning("restarting browser-pc")
        chrome = self.check_chrome()
        if not chrome.ok:
            chrome = self.restart_chrome()
        if not chrome.ok:
            return CheckResult(False, f"Chrome unavailable before browser-pc start: {chrome.detail}")

        self.terminate_browser_pc()
        token = self.load_custodian_token()
        environment = os.environ.copy()
        if token:
            environment["CUSTODIAN_MCP_TOKEN"] = token
        try:
            with Path("/tmp/browser_service.log").open("a", encoding="utf-8") as log_file:
                subprocess.Popen(
                    ["python3", "server.py"],
                    cwd=BROWSER_PC_DIR,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        except OSError as exc:
            return CheckResult(False, f"browser-pc launch failed: {type(exc).__name__}")

        for _ in range(10):
            self.sleep(5)
            health = self.check_browser_pc()
            if health.ok:
                return CheckResult(True, "browser-pc healthy after restart")
        return CheckResult(False, "browser-pc did not pass health and capture within 50s")

    def terminate_browser_pc(self) -> None:
        for pid in self.matching_browser_pc_pids():
            try:
                os.kill(pid, signal.SIGTERM)
                self.log.info("stopped browser-pc pid %s", pid)
            except ProcessLookupError:
                continue
        self.sleep(2)

    def matching_browser_pc_pids(self) -> list[int]:
        result = self.command_runner(
            ["pgrep", "-f", "python3 server.py"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode != 0:
            return []
        pids = []
        for value in result.stdout.splitlines():
            if not value.isdigit():
                continue
            pid = int(value)
            try:
                if Path(os.readlink(f"/proc/{pid}/cwd")) == BROWSER_PC_DIR:
                    pids.append(pid)
            except OSError:
                continue
        return pids

    def restart_strategy_b(self) -> CheckResult:
        return self.start_process(
            "Strategy B",
            ["python3", "scripts/run_strategy_b.py"],
            PROJECT_ROOT,
            Path("/tmp/strategy_b.log"),
            "run_strategy_b.py",
        )

    def restart_telegram_bot(self) -> CheckResult:
        return self.start_process(
            "Telegram bot",
            ["node", "bot.js"],
            PROJECT_ROOT / "bot",
            Path("/tmp/telegram_bot.log"),
            "node bot.js",
            {"NODE_NO_WARNINGS": "1"},
        )

    def start_process(
        self,
        name: str,
        command: list[str],
        cwd: Path,
        log_path: Path,
        pattern: str,
        extra_env: dict[str, str] | None = None,
    ) -> CheckResult:
        self.log.warning("restarting %s", name)
        environment = os.environ.copy()
        if extra_env:
            environment.update(extra_env)
        try:
            with log_path.open("a", encoding="utf-8") as log_file:
                subprocess.Popen(
                    command,
                    cwd=cwd,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        except OSError as exc:
            return CheckResult(False, f"launch failed: {type(exc).__name__}")

        self.sleep(3)
        return self.check_process(pattern)

    def request_json(self, url: str, *, method: str, timeout: float) -> dict[str, object]:
        request = Request(url, method=method)
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise URLError(f"HTTP {response.status}")
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise json.JSONDecodeError("response is not an object", "", 0)
        return payload

    def load_custodian_token(self) -> str | None:
        token = os.environ.get("CUSTODIAN_MCP_TOKEN")
        if token:
            return token
        unit_path = Path.home() / ".config/systemd/user/custodian-mcp-http.service"
        try:
            text = unit_path.read_text(encoding="utf-8")
        except OSError:
            return None
        match = re.search(r"CUSTODIAN_MCP_TOKEN=([a-f0-9]+)", text)
        return match.group(1) if match else None

    def report_failure(self, component: str, detail: str, recovered: bool) -> None:
        outcome = "restart succeeded" if recovered else "restart FAILED"
        message = f"MT health monitor: {component} unhealthy ({detail}); {outcome}."
        self.log.warning(message)
        self.send_telegram_alert(message)

    def send_telegram_alert(self, message: str) -> None:
        credentials = self.telegram_credentials()
        if credentials is None:
            self.log.error("Telegram alert skipped: token/chat ID unavailable")
            return
        token, chat_id = credentials
        try:
            result = self.command_runner(
                [
                    "curl",
                    "--silent",
                    "--show-error",
                    "--max-time",
                    "15",
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    "--data-urlencode",
                    f"chat_id={chat_id}",
                    "--data-urlencode",
                    f"text={message}",
                ],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.log.error("Telegram alert request failed: %s", type(exc).__name__)
            return
        if result.returncode == 0 and '"ok":true' in result.stdout:
            self.log.info("Telegram alert delivered")
        else:
            self.log.error("Telegram alert request returned exit code %s", result.returncode)

    def telegram_credentials(self) -> tuple[str, str] | None:
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if token and chat_id:
            return token, chat_id
        try:
            config = json.loads(BOT_CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        token = config.get("telegram_token")
        chat_id = config.get("telegram_chat_id")
        if isinstance(token, str) and token and isinstance(chat_id, (str, int)) and str(chat_id):
            return token, str(chat_id)
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Restart unhealthy memecoin runtime components.")
    parser.add_argument("--once", action="store_true", help="Run one health-check cycle (the default).")
    parser.parse_args()

    with LOCK_PATH.open("w", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        HealthMonitor().run_cycle()
    return 0


if __name__ == "__main__":
    sys.exit(main())
