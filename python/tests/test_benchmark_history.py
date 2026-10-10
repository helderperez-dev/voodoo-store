from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _benchmark_module():
    path = Path(__file__).parents[2] / "scripts" / "benchmark_store.py"
    spec = importlib.util.spec_from_file_location("voodoo_store_benchmark", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_benchmark_history_is_schema_versioned_and_append_only(tmp_path: Path) -> None:
    module = _benchmark_module()
    history = tmp_path / "benchmarks" / "history.jsonl"
    result = {"iterations": 10, "elapsed_ms": 2.5, "commits": 10}

    first = module.append_history(history, result)
    second = module.append_history(history, result)

    records = [json.loads(line) for line in history.read_text().splitlines()]
    assert len(records) == 2
    assert first["schema_version"] == module.BENCHMARK_SCHEMA_VERSION
    assert second["recorded_at"]
    assert all(record["elapsed_ms"] == 2.5 for record in records)


def test_benchmark_history_rejects_corrupt_prior_records(tmp_path: Path) -> None:
    module = _benchmark_module()
    history = tmp_path / "history.jsonl"
    history.write_text('{"schema_version": 999, "elapsed_ms": 1}\n')

    with pytest.raises(ValueError, match="unsupported benchmark history schema"):
        module.append_history(history, {"elapsed_ms": 2})


def test_benchmark_history_trend_reports_change_from_previous_sample(tmp_path: Path) -> None:
    module = _benchmark_module()
    history = tmp_path / "history.jsonl"
    module.append_history(history, {"elapsed_ms": 100.0})
    module.append_history(history, {"elapsed_ms": 110.0})
    module.append_history(history, {"elapsed_ms": 90.0})

    summary = module.summarize_history(history)

    assert summary == {
        "schema_version": module.BENCHMARK_SCHEMA_VERSION,
        "samples": 3,
        "min_elapsed_ms": 90.0,
        "max_elapsed_ms": 110.0,
        "mean_elapsed_ms": 100.0,
        "latest_elapsed_ms": 90.0,
        "change_from_previous_ms": -20.0,
        "change_from_previous_percent": -18.182,
    }


def test_benchmark_history_trend_rejects_empty_history(tmp_path: Path) -> None:
    module = _benchmark_module()
    history = tmp_path / "empty.jsonl"
    history.write_text("\n")

    with pytest.raises(ValueError, match="history is empty"):
        module.summarize_history(history)
