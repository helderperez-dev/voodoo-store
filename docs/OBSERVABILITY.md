# Store observability contract

`docs/OBSERVABILITY_CONTRACT.json` is the retained dashboard and alert
contract for Voodoo Store operations. The portable deployable dashboard is
checked in as `docs/OBSERVABILITY_DASHBOARD.json`; it intentionally describes signals from
the public `health_report()` surface rather than reaching into private file or
queue encodings. The same names are exposed by the Rust health report, the
Python binding, and the CLI health command.

`docs/OBSERVABILITY_GRAFANA_DASHBOARD.json` is the deterministic Grafana export
of that contract. Regenerate it with
`python scripts/export_grafana_dashboard.py --output docs/OBSERVABILITY_GRAFANA_DASHBOARD.json`;
the dashboard validator verifies that every Prometheus query and alert remains
identical to the portable contract before deployment.

The contract groups the signals into storage growth, queue health, durability
latency, and recovery/object panels. It also defines conservative first-alert
thresholds for dead-lettered work, commit failures, and compaction backlog.
Operators can map these signals to Prometheus/Grafana, an OpenTelemetry
collector, or a periodic JSON health poll without changing application code.

Run `python scripts/check_observability_contract.py` and
`python scripts/check_observability_dashboard.py` after changing the health
report or dashboard. Store CI runs both checks before compiling the workspace,
so a renamed or removed signal cannot silently leave an operational dashboard
behind.

The scheduled performance workflow restores the most recent benchmark history
from the GitHub Actions cache, appends the new sample, uploads the trend, and
saves the expanded history under the immutable run key. This preserves
cross-run trend data without committing generated measurements to source.
