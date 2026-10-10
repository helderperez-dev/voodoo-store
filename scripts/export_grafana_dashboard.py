"""Export the Store observability contract as a deterministic Grafana dashboard."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CONTRACT = ROOT / "docs" / "OBSERVABILITY_CONTRACT.json"
DASHBOARD = ROOT / "docs" / "OBSERVABILITY_DASHBOARD.json"


def build_dashboard(contract: dict[str, object], portable: dict[str, object]) -> dict[str, object]:
    panels: list[dict[str, object]] = []
    for index, panel in enumerate(portable["panels"]):
        queries = panel["queries"]
        panels.append(
            {
                "id": index + 1,
                "title": panel["title"],
                "type": "timeseries",
                "datasource": {"type": "prometheus", "uid": portable["datasource"]},
                "gridPos": {"h": 8, "w": 12, "x": (index % 2) * 12, "y": (index // 2) * 8},
                "targets": [
                    {"refId": chr(65 + query_index), "expr": query, "legendFormat": query}
                    for query_index, query in enumerate(queries)
                ],
                "fieldConfig": {"defaults": {"unit": panel["unit"]}, "overrides": []},
            }
        )
    return {
        "uid": "voodoo-store-operations",
        "title": portable["title"],
        "schemaVersion": 39,
        "version": 1,
        "refresh": f"{portable['refresh_seconds']}s",
        "timezone": "utc",
        "datasource": {"type": "prometheus", "uid": portable["datasource"]},
        "panels": panels,
        "voodooContract": {
            "schema_version": contract["schema_version"],
            "alerts": contract["alerts"],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    portable = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    output = build_dashboard(contract, portable)
    args.output.write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"wrote Grafana dashboard with {len(output['panels'])} panels")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
