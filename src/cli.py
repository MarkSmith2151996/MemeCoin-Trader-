"""Typer CLI entrypoint."""

from __future__ import annotations

import typer
from rich.console import Console

from src.core.config import load_settings
from src.monitoring.health import check_health

app = typer.Typer(help="Memecoin Trader CLI")
console = Console()


@app.command()
def health() -> None:
    status = check_health()
    console.print({"ok": status.ok, "message": status.message, "checked_at": status.checked_at})


@app.command("show-config")
def show_config() -> None:
    settings = load_settings()
    console.print(settings.model_dump())


if __name__ == "__main__":
    app()
