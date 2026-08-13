#!/usr/bin/env python3
"""Download completed Dune executions as CSV without storing API credentials."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


API_URL = "https://api.dune.com/api/v1"


def request_json(url: str, api_key: str, *, method: str = "GET", body: object | None = None) -> dict:
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    request = Request(url, data=payload, method=method)
    request.add_header("X-Dune-API-Key", api_key)
    if payload is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urlopen(request, timeout=120) as response:  # noqa: S310 - fixed Dune API URL
            return json.load(response)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Dune API {method} {url} failed: {exc.code} {detail}") from exc


def create_query(sql_path: Path, api_key: str, *, is_private: bool) -> int:
    sql = sql_path.read_text(encoding="utf-8")
    query_b_marker = "-- QUERY B:"
    if query_b_marker in sql:
        sql = sql[sql.index("WITH graduations AS", sql.index(query_b_marker)):]
    response = request_json(
        f"{API_URL}/query",
        api_key,
        method="POST",
        body={
            "name": "MT-534 graduated token swaps",
            "description": "Detached two-hour price paths for the Memecoin Trader Dune backtest.",
            "is_private": is_private,
            "query_sql": sql,
        },
    )
    return int(response["query_id"])


def update_query(query_id: int, sql_path: Path, api_key: str) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    query_b_marker = "-- QUERY B:"
    if query_b_marker in sql:
        sql = sql[sql.index("WITH graduations AS", sql.index(query_b_marker)):]
    request_json(
        f"{API_URL}/query/{query_id}",
        api_key,
        method="PATCH",
        body={"query_sql": sql},
    )


def execute_query(query_id: int, api_key: str) -> str:
    response = request_json(f"{API_URL}/query/{query_id}/execute", api_key, method="POST")
    return str(response["execution_id"])


def wait_for_execution(execution_id: str, api_key: str) -> dict:
    url = f"{API_URL}/execution/{execution_id}/status"
    while True:
        status = request_json(url, api_key)
        state = status.get("state")
        if status.get("is_execution_finished"):
            if state != "QUERY_STATE_COMPLETED":
                raise RuntimeError(f"Dune execution {execution_id} ended in {state}")
            return status
        time.sleep(5)


def download_execution(execution_id: str, output_path: Path, api_key: str) -> int:
    rows: list[dict[str, object]] = []
    columns: list[str] | None = None
    offset = 0
    while True:
        response = request_json(
            f"{API_URL}/execution/{execution_id}/results?limit=1000&offset={offset}", api_key,
        )
        result = response["result"]
        metadata = result["metadata"]
        columns = columns or list(metadata["column_names"])
        page = list(result["rows"])
        rows.extend(page)
        total = int(metadata["total_row_count"])
        if len(rows) >= total:
            break
        if not page:
            raise RuntimeError(f"Dune execution {execution_id} returned an empty page at {offset}")
        offset += len(page)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns or [])
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key", default=os.environ.get("DUNE_API_KEY"))
    parser.add_argument("--execution-id", help="Completed execution to download.")
    parser.add_argument("--query-sql", type=Path, help="Create, execute, and download this Dune SQL query.")
    parser.add_argument("--query-id", type=int, help="Update this existing query with --query-sql before execution.")
    parser.add_argument("--public-query", action="store_true", help="Create the query publicly when private-query quota is exhausted.")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise SystemExit("Set DUNE_API_KEY or pass --api-key.")
    if bool(args.execution_id) == bool(args.query_sql):
        raise SystemExit("Provide exactly one of --execution-id or --query-sql.")
    if args.query_id and not args.query_sql:
        raise SystemExit("--query-id requires --query-sql.")

    if args.query_sql:
        if args.query_id:
            query_id = args.query_id
            update_query(query_id, args.query_sql, args.api_key)
        else:
            query_id = create_query(args.query_sql, args.api_key, is_private=not args.public_query)
        execution_id = execute_query(query_id, args.api_key)
        status = wait_for_execution(execution_id, args.api_key)
        print(f"Query {query_id} completed as execution {execution_id}.")
        print(f"Dune execution time: {status.get('execution_ended_at', 'unknown')}.")
    else:
        execution_id = args.execution_id

    count = download_execution(execution_id, args.output, args.api_key)
    print(f"Wrote {count} rows to {args.output}")


if __name__ == "__main__":
    main()
