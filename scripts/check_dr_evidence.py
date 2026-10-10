"""Validate JSON evidence emitted by the Store disaster-recovery drill."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

SHA256 = re.compile(r"^[0-9a-f]{64}$")


def validate(evidence: dict[str, object]) -> None:
    required = {
        "status",
        "restored_keys",
        "compacted_keys",
        "format",
        "manifest_version",
        "manifest_store_id",
        "manifest_sha256",
        "fixture",
        "artifact_version",
        "artifact_record_sha256",
    }
    missing = sorted(required - evidence.keys())
    if missing:
        raise ValueError("DR evidence missing fields: " + ", ".join(missing))
    if evidence["status"] != "passed":
        raise ValueError("DR evidence status is not passed")
    for field in ("restored_keys", "compacted_keys", "manifest_version"):
        value = evidence[field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"DR evidence field {field} must be a non-negative integer")
    if evidence["restored_keys"] < 25 or evidence["compacted_keys"] < 25:
        raise ValueError("DR evidence reports fewer than the required durable records")
    if not isinstance(evidence["format"], str) or not re.fullmatch(r"\d+\.\d+", evidence["format"]):
        raise ValueError("DR evidence format must be major.minor")
    if not isinstance(evidence["manifest_store_id"], str) or not evidence["manifest_store_id"]:
        raise ValueError("DR evidence must include a Store identity")
    digest = evidence["manifest_sha256"]
    if not isinstance(digest, str) or not SHA256.fullmatch(digest):
        raise ValueError("DR evidence manifest_sha256 must be lowercase SHA-256")
    fixture = evidence["fixture"]
    if fixture is not None and (not isinstance(fixture, str) or not fixture.endswith(".vstore")):
        raise ValueError("DR evidence fixture must be a .vstore path or null")
    artifact_version = evidence["artifact_version"]
    if not isinstance(artifact_version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", artifact_version):
        raise ValueError("DR evidence must include the installed Store artifact version")
    record_digest = evidence["artifact_record_sha256"]
    if not isinstance(record_digest, str) or not SHA256.fullmatch(record_digest):
        raise ValueError("DR evidence must include the installed artifact RECORD SHA-256")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    try:
        validate(json.loads(args.evidence.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise SystemExit(str(error)) from error
    print("DR evidence contract passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
