# Deploy assets

- `grafana/sierpe-dashboard.json` — import into Grafana (Dashboards →
  Import), point it at the Prometheus datasource that scrapes `/metrics`.
  Panels cover tip lag, gaps, pending backfills, suppression counters,
  ingestion and extraction rates, commit latency percentiles and source
  failovers. Every metric is documented in
  [../docs/METRICS.md](../docs/METRICS.md).
- `gatus/config.yaml` — a public status page for your instance (replace
  YOUR-INSTANCE). Health, readiness, tip lag and the open-gaps honesty
  check.

Deployment paths for the appliance itself live in
[../docs/DEPLOY.md](../docs/DEPLOY.md).
