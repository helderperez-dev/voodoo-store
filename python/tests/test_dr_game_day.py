from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from voodoo_store import __version__


def test_game_day_accepts_retained_release_fixture() -> None:
    root = Path(__file__).parents[2]
    fixture = root / "tests" / "fixtures" / "legacy-0.2.2.vstore"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "dr_game_day.py"),
            "--fixture",
            str(fixture),
            "--validate-evidence",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    evidence = json.loads(result.stdout)
    assert evidence["status"] == "passed"
    assert evidence["artifact_version"] == __version__
    assert evidence["fixture"].endswith("legacy-0.2.2.vstore")
    assert evidence["restored_keys"] >= 26
    assert evidence["compacted_keys"] >= 26


def test_game_day_rejects_mismatched_artifact_version() -> None:
    root = Path(__file__).parents[2]
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "dr_game_day.py"),
            "--artifact-version",
            "9.9.9",
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "does not match requested 9.9.9" in result.stderr


def test_game_day_evidence_contract_rejects_incomplete_report() -> None:
    import importlib.util

    root = Path(__file__).parents[2]
    path = root / "scripts" / "check_dr_evidence.py"
    spec = importlib.util.spec_from_file_location("dr_evidence", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(ValueError, match="missing fields"):
        module.validate({"status": "passed"})
