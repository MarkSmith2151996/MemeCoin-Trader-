"""V2 kill-script process boundary coverage."""

from __future__ import annotations

import subprocess

from scripts import kill_switch


def test_kill_switch_uses_noninteractive_sudo_for_service_stop(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 3 if "is-active" in command else 0, "", "")

    monkeypatch.setattr(kill_switch.shutil, "which", lambda name: "/usr/bin/systemctl")
    monkeypatch.setattr(kill_switch.subprocess, "run", fake_run)

    kill_switch.ensure_executor_stopped(force=False)

    assert calls == [
        ["sudo", "-n", "/usr/bin/systemctl", "stop", "memecoin-executor.service"],
        [
            "sudo",
            "-n",
            "/usr/bin/systemctl",
            "is-active",
            "--quiet",
            "memecoin-executor.service",
        ],
    ]
