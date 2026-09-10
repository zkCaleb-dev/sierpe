# Changelog

All notable changes to Sierpe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org).

## [Unreleased]

## [1.10.1] - 2026-09-10

### Fixed

- Recording a gap now subtracts the whole set of open gaps instead of
  trimming its floor upward, so it can produce several rows. The old
  walk stopped at the first ledger no open gap covered, which a heal
  plan guarantees: every cluster the healer resolves leaves a hole in
  the open coverage, and the next registration batch to clamp recorded
  one gap from that hole to its wall — overlapping every gap still open
  above it, and sending the healer to replay linearly exactly the
  deserts the plan had just excluded. That is the overlap the trimming
  was introduced to prevent, in the shape sparse healing gave it.
- Re-recording a range whose gap was already healed reopens that gap
  instead of silently doing nothing. The deterministic gap id collided
  with the resolved row, so a contract registered after a heal was never
  promised the range again and its coverage claimed history nobody had
  derived for it. Resolved gaps were healed against the registry as it
  stood then, so a later registration is owed a fresh replay.

## [1.10.0] - 2026-09-10

### Added

- Sparse healing: `POST /v1/admin/gaps/plan` reconciles the open gaps
  against a replay plan, splitting each one into the ranges the archive
  leg will replay and the ranges it will not. Deferred ranges stay open
  and declared — the plan is scheduling, never a claim, so a wrong plan
  costs coverage and can never state that history was indexed when it
  was not. Deep heals were the motive: replaying a 6.2M-ledger gap
  linearly measured out at two weeks for activity that lived in 0.15% of
  it, and planning the same gap brings it to about four days on one
  worker.
  Registering a contract reopens every deferred gap covering its
  history, because the plan that deferred them was computed for a
  contract set that did not include it. `/status` and the new
  `sierpe_deferred_gaps` and `sierpe_deferred_ledgers` gauges break the
  deferred share out of the open-gap count, so a plan does not read as
  damage. See `docs/SPARSE-HEAL.md`.

  **Operators watching `open_gaps` for completion must switch to
  `gaps_pending_heal`.** Deferred gaps stay open on purpose, so once a plan
  is applied `open_gaps` no longer reaches zero and anything checking
  `open_gaps == 0` waits forever without erroring. `/status` now serves
  `gaps_pending_heal` (open minus deferred) and the equivalent for
  Prometheus is `sierpe_open_gaps - sierpe_deferred_gaps`. Instances that
  never apply a plan are unaffected.
- `HEAL_WORKERS` replays several gaps at once (default 1, max 16). The
  equivalence gate still runs exactly once, before any worker starts, so
  no worker can commit a replay nobody proved; a gap is claimed while a
  chunk of it is in flight, so two workers never replay the same range or
  race on its watermark. Each worker is its own captive core, which makes
  memory the binding constraint: budget about 10 GB per worker against the
  machine's total RAM minus everything else it runs, since a core killed
  mid-chunk costs that chunk's entire replay. A 16 GB host running anything
  else affords one worker. It also only pays off with several open gaps to
  spread across, which is what a heal plan produces.

### Fixed

- Both images now carry `org.opencontainers.image.source`, which is what
  links a published package to its repository. Without it GHCR kept the
  package detached through 117 versions: it never showed on the repo page
  and never inherited its visibility.
- The `-full` image pins an exact stellar-core build instead of the
  floating `28` tag. That tag moved from 28.0.0 to 28.0.1 mid-pilot, so
  the same Dockerfile silently produced a different replay engine
  depending on the build date — in the one image whose job is to
  reproduce history byte-for-byte. The pin is now bumped deliberately as
  part of a release, and the equivalence gate re-proves each new build.
- `docs/RELEASING.md` now includes moving `latest`, which nothing does on
  its own. It had stayed on v1.5.2 through four releases, so every
  `docker pull` without an explicit tag served an image missing the
  backfill fixes from 1.6.0 through 1.9.0. The tag has been corrected;
  anyone who pulled `latest` since 2026-09-07 should pull again.

## [1.9.0] - 2026-09-08

### Fixed

- Heal chunks grew from 2,000 to 100,000 ledgers (new `HEAL_CHUNK_LEDGERS`
  to tune). Every chunk is a fresh captive core run, and the SDK gives each
  bounded catchup an ephemeral working directory it deletes on close, so a
  chunk re-downloads the full bucket set of its anchor checkpoint every
  time — a fixed multi-minute cost that made deep heals spend days on
  redundant downloads (a 6M-ledger gap paid it ~3,100 times; on a
  residential link that modeled out to weeks). The chunk size is what
  amortizes that cost; it also bounds the records held in memory before
  the chunk's single atomic commit, so the knob trades download overhead
  against RAM and lost replay work on a crash.

## [1.8.0] - 2026-09-07

### Fixed

- The events cursor now carries and enforces its kind. Every other
  endpoint stamped its cursors, but `/events` — the oldest codec — never
  did, so a cursor minted by a different endpoint whose fields happened
  to unmarshal was accepted (found live: a movements cursor paged
  `/events` with a 200). New events cursors carry `kind: events`; a
  foreign kind is rejected with 400; cursors minted before the stamp
  carry no kind and remain valid, so nothing in the wild breaks.

### Changed

- New gaps are trimmed against the open ones below them, and a resolving
  gap hands its clamped registrations to the deepest gap still open.
  Registrations arrive in batches over days and every batch clamps at
  its own ever-rising retention wall; untrimmed gaps overlapped on
  everything below the previous wall, so the archive leg replayed the
  same deep history once per batch — for a staged mainnet backfill that
  tripled the captive-core replay. An open gap is a standing promise to
  heal its range, so the trim leaves the un-vouched set exactly as it
  was, and the handoff keeps a late batch's declared coverage descending
  with the deeper heal instead of freezing at the shared floor.

## [1.7.0] - 2026-09-07

### Fixed

- The getLedgers batch shrink (1.5.1) had no memory: every call restarted
  at the full batch size and re-paid the aborted oversized downloads
  against the body cap, silently multiplying the bandwidth of a
  heavy-range walk roughly four times (found on the first mainnet homelab
  deployment: ~1.75 MB of meta per ledger near the tip, ~3.5 GB
  downloaded before the first chunk could commit). The client now
  remembers the batch size that fit and probes 25% higher only after
  every 8 successful batches, so heavy ranges pay at most one aborted
  body per 8 good batches and quiet ranges earn their big batches back.

### Changed

- Backfill walks now share their scans. Chunks sit on an absolute
  2000-ledger grid, and contracts whose next chunk is the same range form
  a group whose ledgers are fetched and extracted once — each contract
  still commits its own rows and watermark atomically, so a failed commit
  isolates to its contract while the rest keep their progress; the
  trailing contract retries through a one-entry scan cache instead of
  re-downloading the range. A contract walking alone is a group of one
  and behaves as before. This is what makes registering many contracts at
  once affordable: N same-range registrations previously cost N copies of
  every RPC download. `sierpe_backfill_chunks_total` and
  `sierpe_backfill_ledgers_scanned_total` now count scans actually
  performed (once per shared chunk), which is what they always claimed to
  measure.

## [1.6.0] - 2026-09-07

### Added

- Movements now store and serve the raw ContractEvent XDR they were
  decoded from (`rawXdr` on `/v1/contracts/{id}/movements`, migration
  0011). The emitting token is usually not registered, so no events row
  exists to join to and the original bytes were unrecoverable from the
  database; consumers that re-emit movements into their own pipelines
  need the event itself, not the decode. Rows ingested before the
  migration have no stored event to backfill from and omit the field.
- `POST /v1/contracts` with explicit `kinds` now accepts a contract whose
  instance is no longer live on chain (archived after TTL expiry — the
  RPC cannot tell that apart from one that never existed). It registers
  as classification `unknown` and the response carries a new `warnings`
  array saying so; history is still derived from whatever sources reach,
  which is the point: an archived contract's past exists in the History
  Archives even when its instance does not. Without explicit kinds the
  404 stands, since the kinds default is derived from the classification.
  Re-registering after a restore re-classifies.

### Security

- Toolchain pinned to go1.25.13 and vulnerable indirect dependencies
  upgraded (grpc 1.82.1, aws-sdk-go-v2 eventstream 1.7.8 / s3 1.97.3):
  clears every reachable finding govulncheck reported against binaries
  built from go1.25.0.

## [1.5.2] - 2026-08-21

### Added

- `sierpe healthcheck`: probes the local `/health` endpoint and exits
  0 or 1, and both images now declare it as their `HEALTHCHECK`. The
  image is distroless — no shell, no curl — so every platform that runs
  health checks inside the container (Docker, Swarm, Coolify, Dokploy,
  CapRover, NAS container managers) marked Sierpe unhealthy forever and,
  on some of them, refused to route traffic to it. Kubernetes and cloud
  load balancers probe over the network and are unaffected.
- When the process dies with the signature of a transaction-mode
  connection pooler that lacks prepared-statement support (SQLSTATE
  26000 or 42P05), the final log line now carries the one-parameter fix
  (`default_query_exec_mode=simple_protocol`) instead of leaving the
  operator to discover pgx internals. Found while mapping which Postgres
  providers Sierpe works against: Supabase's transaction pooler, older
  PgBouncer and several managed pools all fail exactly this way.

### Fixed

- Sierpe did not work under pgx's simple protocol, which is what a user
  has to select (`default_query_exec_mode=simple_protocol` in
  `DATABASE_URL`) to sit behind a transaction-mode pooler that does not
  support prepared statements. Both jsonb writers relied on the extended
  protocol's type hints: event topics went out as a Postgres array
  literal and a contract's classification as bytea hex, and jsonb
  rejected both with `invalid input syntax for type json`. Both now send
  JSON text explicitly, which behaves identically in either protocol, and
  the store suite runs under the simple protocol as a regression test.
  Found while mapping which Postgres providers Sierpe can run against.

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
