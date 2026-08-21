# Changelog

All notable changes to Sierpe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org).

## [Unreleased]

## [1.5.1] - 2026-08-21

### Fixed

- **A backfill could stall permanently on a busy ledger range.** The RPC
  client capped response bodies with an `io.LimitReader`, which stops
  reading without reporting that it did — so an oversized answer reached
  the decoder as a perfectly truncated document and surfaced as
  `unexpected end of JSON input`, the client blaming the server for its
  own cut. The failure is deterministic: the same request produces the
  same oversized reply forever, so the walk retried it every 40 seconds
  and never advanced again, while coverage kept reporting the backfill as
  merely pending. Found on the live deployment, where 200 ledgers of a
  busy testnet range weigh ~151 MB against a 64 MB cap.

  The client now detects the overflow and names it, and `GetLedgerBatch`
  halves its request down to a single ledger until the answer fits —
  ledger meta size is data-dependent and unbounded, so no fixed batch size
  is safe. An oversized answer no longer burns the endpoint pool either:
  every endpoint would send the same bytes.

- The backfill logged a WARN saying a chunk had FAILED every time a
  contract was registered. Nothing had failed: the anchor now sits a margin
  past the live cursor, so the first chunk asks for ledgers that have not
  closed yet and the source correctly refuses them. It resolves itself as
  the tip advances. A log line that cries wolf on a routine operation
  trains the operator to ignore the line that means something, so this is
  now an INFO saying what is actually happening. Found by watching the
  logs of the first real deployment right after registering a kind.

### Documentation

- `Coverage.indexedFromLedger` can legitimately exceed `indexedToLedger`
  for about a reload interval after a registration: the backfill anchor
  now sits past the live cursor, so until ingestion reaches it the covered
  window is EMPTY rather than inverted-by-mistake. The scan statuses are
  correct throughout (a bounded query below the frontier gets
  `OLDEST_REACHED`, not `COMPLETE`), but a client computing a span from
  the two numbers gets a negative, so the spec now says so. Found by
  smoke-testing the v1.5.0 image against testnet.

## [1.5.0] - 2026-08-21

### Added

- **Movements (`movements` kind)**: token transfers a registered contract
  takes part in, as sender or recipient, whoever emitted them. This is the
  resource that answers "what came into and went out of my contract":
  paying a contract emits the transfer from the ASSET's own SAC, so those
  movements now land without the operator registering that asset at all.
  Served at `GET /v1/contracts/:id/movements` with `role`, `token`, `type`
  and ledger-bound filters, an endpoint-bound cursor, and the usual
  scanStatus and coverage. Because ingestion already downloads whole
  ledgers, the descending backfill derives a contract's movement history
  from BEFORE it was registered — the thing dynamic-source indexers
  cannot do.

  Notes on purpose: the kind is bidirectional and is deliberately not
  called "deposits", because a one-directional total reads exactly like a
  balance and is indistinguishable from one until the first outflow that
  never shows up. The response says so in a `note` field. The asset's
  identity is the emitting contract id, never the SEP-0011 asset string.
  Movement-named events from unwatched tokens that fail to decode are
  counted in their own non-alerting metric so the existing suppression
  alarms stay meaningful.

  Reviewed adversarially before merging, which cost the feature its worst
  bug: a movement row is keyed by (transfer_id, role) because one self
  transfer produces two attributions, but the page cursor only carried the
  id — so a page boundary landing between those two rows dropped one of
  them permanently, with no gap and no counter to show for it. The cursor
  now carries the whole row key.

### Fixed

- Coverage is now declared per **(contract, kind)** instead of per
  contract. A backfill walk only derives the kinds the registration
  carried while it ran, so adding a kind to an existing registration left
  its history underived while the API kept declaring `COMPLETE` over
  ledgers it had never looked at for that kind — a rule 7 violation
  reachable by a one-word edit. Now the walk records `covered_kinds`
  (migration 0009), adding a kind reopens the walk at the current anchor,
  and every coverage object names its `kind` and says whether the
  registration derives it at all (`kindDerived`). An endpoint whose kind
  is not derived vouches for nothing instead of implying emptiness means
  absence.
- `docs/openapi.yaml` declared `/v1/contracts` twice (one mapping key for
  POST, another for GET); a strict YAML parser kept only the second, so
  the registration endpoint vanished from generated clients.
- Page cursors trusted the limit they carried. A cursor is opaque, not
  authenticated: anyone can mint one, and a negative limit arrived at the
  store as a slice bound and panicked the request goroutine, while an
  oversized one quietly bypassed the endpoint maximum. All five decoders
  (events, state, transfers, trustlines, movements) now refuse a limit no
  handler would ever mint.
- Registering a contract anchored its backfill exactly at the live cursor,
  but live ingestion only starts deriving that contract once the ingesting
  process reloads its registry. Every ledger closing inside that window
  was derived by nobody while coverage counted it as covered. The anchor
  now sits past the cursor by a margin wider than the reload interval;
  overlapping costs nothing because every insert path is idempotent.
- Dropping a kind from an existing registration left `covered_kinds`
  vouching for it, so removing and re-adding a kind claimed history that
  was never walked with it. Narrowing is now recorded — without reopening
  a finished walk, since a smaller claim needs no new work.

### Changed

- **Breaking (read API):** `GET /v1/contracts/{id}` now returns `coverage`
  as an array with one declaration per registered kind, rather than a
  single object. Per-page coverage on the other endpoints keeps its shape
  and gains the `kind` and `kindDerived` fields.

- UI: registering a non-SAC contract with the trustlines kind now warns
  that the kind only applies to classic assets, and empty events or
  transfers pages explain that plain payments are recorded under the
  asset's own SAC contract — both straight from the first user's test
  session.

## [1.4.2] - 2026-08-20

### Fixed

- The embedded UI went blank when the page was opened through a URL with
  embedded credentials (https://user:pass@host/): relative fetch URLs
  inherit the document credentials and the Fetch spec rejects them. API
  paths now resolve against location.origin, which never carries
  credentials. Found by the first real user.

## [1.4.1] - 2026-08-20

### Fixed

- With `HTTP_BASIC_AUTH` enabled, admin mutations were impossible: the
  Basic credentials and the admin bearer token share the one
  Authorization header, so a request could never satisfy both layers
  (found by the first real user on the first real deployment). The gate
  now also accepts the admin bearer token as a valid credential — it is
  the higher-privilege secret and its own handler still validates it.

## [1.4.0] - 2026-08-20

### Added

- Optional whole-surface Basic Auth (`HTTP_BASIC_AUTH=user:password`):
  when set, every request — embedded UI, API, `/metrics` — requires the
  credentials; only `/health` and `/ready` stay open for orchestrator
  probes. Browsers prompt natively and the UI inherits the credentials
  with no changes; programmatic clients send the standard header from any
  network. Unset keeps the open-reads model for private-networking
  deployments. Constant-time comparison, boot-time validation, redacted
  from the config printout.

## [1.3.0] - 2026-08-20

### Added

- `GET /v1/contracts`: list every registration with its classification
  and kinds, so consumers (and the new UI) can enumerate what the
  instance watches without knowing ids upfront.
- Embedded management UI at `/`: one self-contained HTML page baked into
  the binary (no build system, no external assets) covering the whole
  surface — live instance status, the contract list with classification,
  coverage and counts, a data explorer with a tab per kind (events,
  transfers, state and its history, trustlines and theirs) with filters
  and cursor pagination, and contract registration/unregistration behind
  an admin-token field the page holds only in memory. Reads work without
  credentials, matching the open-reads access model.

## [1.2.0] - 2026-08-20

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
