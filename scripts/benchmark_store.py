"""Run a small reproducible Store durability/throughput benchmark."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from voodoo_store import Store

BENCHMARK_SCHEMA_VERSION = 1


def append_history(path: Path, result: dict[str, Any]) -> dict[str, Any]:
    """Append one validated benchmark record to a durable JSONL history file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                previous = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid benchmark history line {line_number}") from error
            if previous.get("schema_version") != BENCHMARK_SCHEMA_VERSION:
                raise ValueError(f"unsupported benchmark history schema on line {line_number}")
            if not isinstance(previous.get("elapsed_ms"), (int, float)):
                raise ValueError(f"benchmark history line {line_number} has no elapsed_ms")
    record = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(),
        "platform": sys.platform,
        **result,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return record


def summarize_history(path: Path) -> dict[str, Any]:
    """Return a validated, deterministic trend summary for benchmark history."""
    if not path.exists():
        raise FileNotFoundError(path)
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid benchmark history line {line_number}") from error
        if record.get("schema_version") != BENCHMARK_SCHEMA_VERSION:
            raise ValueError(f"unsupported benchmark history schema on line {line_number}")
        elapsed = record.get("elapsed_ms")
        if not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool):
            raise ValueError(f"benchmark history line {line_number} has no elapsed_ms")
        records.append(record)
    if not records:
        raise ValueError("benchmark history is empty")
    elapsed_values = [float(record["elapsed_ms"]) for record in records]
    latest = elapsed_values[-1]
    previous = elapsed_values[-2] if len(elapsed_values) > 1 else None
    return {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "samples": len(elapsed_values),
        "min_elapsed_ms": round(min(elapsed_values), 3),
        "max_elapsed_ms": round(max(elapsed_values), 3),
        "mean_elapsed_ms": round(sum(elapsed_values) / len(elapsed_values), 3),
        "latest_elapsed_ms": round(latest, 3),
        "change_from_previous_ms": (
            round(latest - previous, 3) if previous is not None else None
        ),
        "change_from_previous_percent": (
            round((latest - previous) / previous * 100, 3)
            if previous not in (None, 0)
            else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=1_000)
    parser.add_argument("--max-ms", type=float, default=None)
    parser.add_argument(
        "--history",
        type=Path,
        default=None,
        help="Append the result to a schema-versioned JSONL history file.",
    )
    parser.add_argument(
        "--result",
        type=Path,
        default=None,
        help="Write the current result as a JSON document.",
    )
    parser.add_argument(
        "--trend",
        type=Path,
        default=None,
        help="Write a validated trend summary from an existing JSONL history file.",
    )
    args = parser.parse_args()
    if args.trend is not None:
        summary = summarize_history(args.trend)
        print(json.dumps(summary, sort_keys=True))
        return 0
    if args.iterations <= 0:
        raise SystemExit("--iterations must be positive")

    with tempfile.TemporaryDirectory(prefix="voodoo-store-benchmark-") as root:
        path = Path(root) / "benchmark.vstore"
        started = time.perf_counter()
        with Store.open(path) as store:
            for index in range(args.iterations):
                store.put(f"benchmark:{index}".encode(), b"x" * 128)
            health = store.health_report()
        elapsed_ms = (time.perf_counter() - started) * 1_000

        with Store.open(path) as reopened:
            if reopened.get(f"benchmark:{args.iterations - 1}".encode()) != b"x" * 128:
                raise SystemExit("benchmark reopen verification failed")

    result = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "iterations": args.iterations,
        "elapsed_ms": round(elapsed_ms, 3),
        "commits": health["metrics"]["commits"],
        "bytes_appended": health["metrics"]["bytes_appended"],
        "format": f"{health['format_major']}.{health['format_minor']}",
    }
    if args.result is not None:
        args.result.parent.mkdir(parents=True, exist_ok=True)
        args.result.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    if args.history is not None:
        append_history(args.history, result)
    print(json.dumps(result, sort_keys=True))
    if args.max_ms is not None and elapsed_ms > args.max_ms:
        raise SystemExit(
            f"benchmark budget exceeded: {elapsed_ms:.3f}ms > {args.max_ms:.3f}ms"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
