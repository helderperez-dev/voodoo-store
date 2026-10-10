"""Validate the portable Store operations dashboard artifact."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CONTRACT = ROOT / "docs/OBSERVABILITY_CONTRACT.json"
DASHBOARD = ROOT / "docs/OBSERVABILITY_DASHBOARD.json"
GRAFANA = ROOT / "docs/OBSERVABILITY_GRAFANA_DASHBOARD.json"


def main() -> int:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    if dashboard.get("schema_version") != 1:
        raise SystemExit("unsupported observability dashboard schema")
    if dashboard.get("endpoint") != "/voodoo/metrics/prometheus":
        raise SystemExit("dashboard must use the public Prometheus endpoint")
    if not isinstance(dashboard.get("refresh_seconds"), int) or dashboard["refresh_seconds"] < 5:
        raise SystemExit("dashboard refresh must be an integer of at least five seconds")

    signal_paths = set(contract["signals"].values())
    metric_names = {
        "voodoo_store_health_details_native_health_" + path.replace(".", "_")
        for path in signal_paths
    }
    panels = dashboard.get("panels")
    if not isinstance(panels, list) or not panels:
        raise SystemExit("dashboard must define panels")
    queries = {query for panel in panels for query in panel.get("queries", [])}
    if queries != metric_names:
        missing = sorted(metric_names - queries)
        extra = sorted(queries - metric_names)
        raise SystemExit(f"dashboard query mismatch; missing={missing}, extra={extra}")

    alert_ids = {alert.get("id") for alert in dashboard.get("alerts", [])}
    contract_alert_ids = {alert.get("id") for alert in contract.get("alerts", [])}
    if alert_ids != contract_alert_ids:
        raise SystemExit("dashboard alerts must match the observability contract")
    for alert in dashboard["alerts"]:
        if alert.get("query") not in metric_names:
            raise SystemExit(f"dashboard alert references unknown metric: {alert.get('id')}")
        if alert.get("condition") != "greater-than" or not isinstance(alert.get("threshold"), (int, float)):
            raise SystemExit(f"invalid dashboard alert: {alert.get('id')}")
        if not isinstance(alert.get("for_seconds"), int) or alert["for_seconds"] <= 0:
            raise SystemExit(f"dashboard alert must have a positive duration: {alert.get('id')}")
    grafana = json.loads(GRAFANA.read_text(encoding="utf-8"))
    if grafana.get("uid") != "voodoo-store-operations" or grafana.get("schemaVersion") != 39:
        raise SystemExit("Grafana dashboard identity/schema is invalid")
    if grafana.get("refresh") != f"{dashboard['refresh_seconds']}s":
        raise SystemExit("Grafana dashboard refresh does not match the portable contract")
    grafana_queries = {
        target.get("expr")
        for panel in grafana.get("panels", [])
        for target in panel.get("targets", [])
    }
    if grafana_queries != queries:
        raise SystemExit("Grafana dashboard queries must match the portable dashboard exactly")
    if grafana.get("voodooContract", {}).get("alerts") != contract.get("alerts"):
        raise SystemExit("Grafana dashboard alerts must match the observability contract")
    print(
        f"observability dashboard passed ({len(queries)} metrics, "
        f"{len(alert_ids)} alerts, Grafana export)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
