# Changelog

All notable changes to Sierpe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org).

## [Unreleased]

Nothing yet.

## [1.1.0] - 2026-08-20

### Added

- Token transfers (`transfers` kind): SEP-41 movement events (transfer,
  mint, burn, clawback) decode into structured rows — from/to addresses,
  exact i128 amount, SEP-0011 asset, CAP-67 destination muxed id — written
  in the same atomic commit as events and state. SAC registrations derive
  transfers by default; custom SEP-41 tokens opt in through `kinds`. A
  movement event that fails to decode is counted
  (`sierpe_suppressed_transfers_total`, alertable) while its raw event row
  still lands.
- `GET /v1/contracts/:id/transfers`: decoded movements in chain order with
  `account`/`from`/`to`/`type` and ledger-bound filters, opaque full-query
  cursors bound to the endpoint, scanStatus and declared coverage — the
  same honesty contract as the events endpoint.
- Classic trustlines (`trustlines` kind, opt-in): trustline changes of the
  asset a SAC wraps are attributed to that SAC (the contract id is derived
  locally — zero extra RPC), stored as full history plus a
  convergence-safe holder snapshot with tombstones, and served at
  `GET /v1/contracts/:id/trustlines` (live holders) and
  `/trustlines/history` (chain-order changes with before/after balances).
  Native XLM has no trustlines; the kind observes issued assets only.

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
