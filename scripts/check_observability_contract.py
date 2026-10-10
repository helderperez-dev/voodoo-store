"""Validate the retained Store health/dashboard signal contract."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CONTRACT = ROOT / "docs/OBSERVABILITY_CONTRACT.json"
PYTHON_BINDING = ROOT / "crates/voodoo-store-python/src/operations.rs"
CLI = ROOT / "crates/voodoo-store-cli/src/main.rs"


def main() -> int:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    if contract.get("schema_version") != 1:
        raise SystemExit("unsupported observability contract schema")
    signals = contract.get("signals")
    if not isinstance(signals, dict) or not signals:
        raise SystemExit("observability contract must define signals")
    source = PYTHON_BINDING.read_text(encoding="utf-8")
    cli = CLI.read_text(encoding="utf-8")
    missing = []
    for name, path in signals.items():
        leaf = path.rsplit(".", 1)[-1]
        if leaf not in source or leaf not in cli:
            missing.append(f"{name} ({path})")
    if missing:
        raise SystemExit("health signals missing from public surfaces: " + ", ".join(missing))
    panels = contract.get("panels", [])
    panel_signals = {
        signal
        for panel in panels
        for signal in panel.get("signals", [])
    }
    if panel_signals != set(signals):
        raise SystemExit("dashboard panels must cover every declared health signal exactly")
    for alert in contract.get("alerts", []):
        if alert.get("signal") not in signals:
            raise SystemExit(f"alert references unknown signal: {alert.get('signal')}")
        if alert.get("condition") != "greater-than" or not isinstance(alert.get("threshold"), (int, float)):
            raise SystemExit(f"invalid alert definition: {alert.get('id')}")
    print(f"observability contract passed ({len(signals)} signals, {len(panels)} panels)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
