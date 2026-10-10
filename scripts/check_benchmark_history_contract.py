"""Validate benchmark history append, trend, and corruption contracts."""

from __future__ import annotations

import importlib.util
import tempfile
from pathlib import Path

ROOT = Path(__file__).parents[1]


def _benchmark_module():
    path = ROOT / "scripts/benchmark_store.py"
    spec = importlib.util.spec_from_file_location("voodoo_store_benchmark_contract", path)
    if spec is None or spec.loader is None:
        raise SystemExit("benchmark module is not importable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    module = _benchmark_module()
    with tempfile.TemporaryDirectory(prefix="voodoo-store-history-") as directory:
        history = Path(directory) / "history.jsonl"
        for elapsed in (100.0, 110.0, 90.0):
            module.append_history(history, {"elapsed_ms": elapsed, "iterations": 1000})
        summary = module.summarize_history(history)
        if summary["samples"] != 3 or summary["latest_elapsed_ms"] != 90.0:
            raise SystemExit("benchmark trend summary has incorrect latest sample")
        if summary["change_from_previous_ms"] != -20.0:
            raise SystemExit("benchmark trend summary has incorrect delta")
        history.write_text(history.read_text(encoding="utf-8") + "not-json\n", encoding="utf-8")
        try:
            module.summarize_history(history)
        except ValueError as error:
            if "invalid benchmark history line" not in str(error):
                raise SystemExit("benchmark corruption error is not actionable") from error
        else:
            raise SystemExit("corrupt benchmark history unexpectedly passed")
    print("benchmark history contract passed (append, trend, corruption rejection)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
