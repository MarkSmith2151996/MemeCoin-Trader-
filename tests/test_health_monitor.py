"""Focused unit coverage for the self-healing runtime monitor."""

from __future__ import annotations

import json
import subprocess

from scripts.health_monitor import CheckResult, HealthMonitor


def test_run_cycle_restarts_each_failed_component(monkeypatch, tmp_path):
    monitor = HealthMonitor(sleep=lambda _: None)
    reported = []

    monkeypatch.setattr("scripts.health_monitor.STRATEGY_B_HALT_PATH", tmp_path / "no-halt")
    monkeypatch.setattr(monitor, "check_chrome", lambda: CheckResult(True, "ok"))
    monkeypatch.setattr(monitor, "check_browser_pc", lambda: CheckResult(True, "ok"))
    monkeypatch.setattr(
        monitor,
        "check_process",
        lambda pattern: CheckResult(pattern != "run_strategy_b.py", "missing"),
    )
    monkeypatch.setattr(monitor, "restart_strategy_b", lambda: CheckResult(True, "started"))
    monkeypatch.setattr(monitor, "report_failure", lambda *args: reported.append(args))

    assert not monitor.run_cycle()
    assert reported == [("Strategy B", "missing", True)]


def test_halt_file_prevents_all_automatic_restarts(monkeypatch, tmp_path) -> None:
    halt_path = tmp_path / "strategy_b_halted"
    halt_path.write_text('{"reason":"test"}\n')
    monitor = HealthMonitor(sleep=lambda _: None)
    restart_attempts = []

    monkeypatch.setattr("scripts.health_monitor.STRATEGY_B_HALT_PATH", halt_path)
    monkeypatch.setattr(monitor, "restart_strategy_b", lambda: restart_attempts.append("strategy"))
    monkeypatch.setattr(monitor, "restart_chrome", lambda: restart_attempts.append("chrome"))
    monkeypatch.setattr(monitor, "restart_browser_pc", lambda: restart_attempts.append("browser"))
    monkeypatch.setattr(
        monitor,
        "restart_telegram_bot",
        lambda: restart_attempts.append("telegram"),
    )

    assert not monitor.run_cycle()
    assert restart_attempts == []


def test_telegram_alert_uses_direct_curl(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"telegram_token": "token", "telegram_chat_id": "chat"}))
    monitor = HealthMonitor(sleep=lambda _: None)
    calls = []

    monkeypatch.setattr("scripts.health_monitor.BOT_CONFIG_PATH", config_path)
    monkeypatch.setattr(
        monitor,
        "command_runner",
        lambda command, **_: calls.append(command)
        or subprocess.CompletedProcess(command, 0, '{"ok":true}', ""),
    )

    monitor.send_telegram_alert("component unhealthy")

    assert calls == [
        [
            "curl",
            "--silent",
            "--show-error",
            "--max-time",
            "15",
            "https://api.telegram.org/bottoken/sendMessage",
            "--data-urlencode",
            "chat_id=chat",
            "--data-urlencode",
            "text=component unhealthy",
        ]
    ]


def test_browser_capture_uses_a_valid_url_and_wait_seconds(monkeypatch):
    monitor = HealthMonitor(sleep=lambda _: None)
    requests = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b'{"status":"ok"}'

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        return Response()

    monkeypatch.setattr(monitor, "request_json", lambda *_args, **_kwargs: {"status": "ok"})
    monkeypatch.setattr("scripts.health_monitor.urlopen", fake_urlopen)

    assert monitor.check_browser_pc().ok
    body = json.loads(requests[0][0].data)
    assert body == {"url": "https://example.com", "wait_seconds": 3}
