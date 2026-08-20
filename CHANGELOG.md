# Changelog

All notable changes to Sierpe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org).

## [Unreleased]

### Added

- Archive leg groundwork: a captive stellar-core replay source
  (`internal/source/captive`) serving bounded history-archive ranges with
  the unified event semantics RPC serves (EMIT_CLASSIC_EVENTS +
  BACKFILL_STELLAR_ASSET_EVENTS), plus boot configuration
  (`STELLAR_CORE_BINARY`, `HISTORY_ARCHIVE_URLS`, `CAPTIVE_STORAGE_PATH`)
  validated at startup.
- Gap healer: with the archive leg enabled, recorded below-retention gaps
  are walked downward in atomic 2000-ledger chunks replayed from the
  archives, with a heal watermark on the gap row and the clamped backfill
  frontiers lowered (and un-clamped) in the same transaction, so declared
  coverage grows exactly as fast as healed data lands. Before the first
  heal the captive replay must prove itself **byte-equivalent to the RPC**
  on a checkpoint-aligned range both can serve, after normalizing the two
  parts of the meta that are unstable run to run even on identical core
  builds (both proven live): diagnostic events are stripped, and
  ledger-entry-change units are canonically ordered within each
  operation. A divergent replay disables healing
  (`sierpe_archive_equivalence_failures_total`, alertable) instead of
  filling gaps with unverified data. `/status`
  reports the leg as `archive: off|unverified|verified|equivalence_failed`,
  and heals progress through `sierpe_gaps_healed_total` and
  `sierpe_healed_ledgers_total`.
- `-full` image variant (`Dockerfile.full`, linux/amd64): the appliance
  plus stellar-core, with `STELLAR_CORE_BINARY` pre-set — deploy it and
  registrations reach below RPC retention out of the box. The slim image
  stays multi-arch and distroless for archive-less deployments.
  Deployment guidance in docs/DEPLOY.md.

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
