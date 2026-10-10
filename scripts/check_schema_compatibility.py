"""Validate the checked-in Store collection schema compatibility policy."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CONTRACT = ROOT / "docs" / "SCHEMA_COMPATIBILITY.json"


def main() -> int:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    if contract.get("schema_contract_version") != 1:
        raise SystemExit("unsupported schema compatibility contract version")
    if contract.get("policy") != "compare-and-swap":
        raise SystemExit("schema compatibility policy must be compare-and-swap")
    if contract.get("rollback_policy") != "forward-only-restore-or-reverse-migration":
        raise SystemExit("schema compatibility rollback policy is invalid")

    collections = contract.get("collections")
    if not isinstance(collections, list) or not collections:
        raise SystemExit("schema compatibility contract must define collections")
    names: set[str] = set()
    transitions = 0
    for collection in collections:
        if not isinstance(collection, dict):
            raise SystemExit("schema compatibility collection must be an object")
        name = collection.get("name")
        codec = collection.get("codec")
        versions = collection.get("versions")
        if not isinstance(name, str) or not name.strip() or name in names:
            raise SystemExit(f"invalid or duplicate schema collection: {name!r}")
        if not isinstance(codec, str) or not codec.strip():
            raise SystemExit(f"schema collection {name!r} has no base codec")
        if not isinstance(versions, list) or not versions:
            raise SystemExit(f"schema collection {name!r} has no transitions")
        names.add(name)
        current: int | None = None
        for step in versions:
            if not isinstance(step, dict):
                raise SystemExit(f"schema collection {name!r} transition is invalid")
            source = step.get("from")
            target = step.get("to")
            target_codec = step.get("codec")
            if (
                isinstance(source, bool)
                or not isinstance(source, int)
                or isinstance(target, bool)
                or not isinstance(target, int)
                or source < 1
                or target <= source
                or not isinstance(target_codec, str)
                or not target_codec.strip()
            ):
                raise SystemExit(f"invalid schema transition in {name!r}: {step!r}")
            if current is not None and source != current:
                raise SystemExit(
                    f"schema transitions for {name!r} are not contiguous: "
                    f"expected {current}, got {source}"
                )
            current = target
            transitions += 1
    print(f"schema compatibility contract passed ({len(names)} collections, {transitions} transitions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
