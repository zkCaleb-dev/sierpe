# Changelog

All notable changes to Sierpe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org).

## [Unreleased]

Nothing yet.

## [1.0.0] - 2026-08-20

First feature-complete cut of the appliance (milestones M0 to M3).

### Added

- Single-writer ingestion loop over a failover pool of Stellar RPC
  endpoints, with permanent hash-chain continuity verification, testnet
  reset detection, and atomic cursor-plus-data commits (exactly-once by
  construction).
- Contract registration API (bearer-authenticated, idempotent) with
  automatic on-chain classification: SAC detection by executable, event
  discovery from the contractspecv0 wasm section, function-name fallback.
- Event extraction into the atomic ledger commit with systematic distrust:
  failed transactions skipped and counted, per-transaction recover
  frontier, suppression counters that alert instead of hiding loss.
- Descending backfill in atomic 2000-ledger chunks with per-chunk
  watermarks, hash continuity inside chunks, and honest clamping at the
  RPC retention wall (the unserved range persists as a gap before the
  clamp commits).
- Contract state: full change history with provenance plus a current
  snapshot guarded against out-of-order replays (tombstones included).
- Public read API: getEvents-v2-compatible events endpoint (positional
  topic filters, opaque full-query cursors, scanStatus vocabulary), state
  snapshot and history endpoints, contract detail with classification,
  derived coverage and counts. Every paginated response declares coverage.
- Operational surface: /health, /ready (503 while catching up), /status,
  Prometheus /metrics (all documented in docs/METRICS.md), Grafana
  dashboard and Gatus status page configs in deploy/.
- Distribution: static distroless container image, docker-compose
  deployment, deployment guide for Railway and generic container
  platforms.

### Security

- Admin surface always authenticated with constant-time token comparison
  and enforced token entropy at boot; secrets redacted from all config
  output.
