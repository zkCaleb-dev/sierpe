# DESIGN — Self-hosted Stellar contract indexer

> Status: foundational draft, pre-code. Codename: `indexer` (final name TBD).
> Companion document: `INDEXER-KNOWLEDGE.md` (the study this design derives
> from — 29 principles P1–P29, each with its source; referenced throughout).

## 1. Vision

A **server** — in the Postgres/Prometheus sense — that any Stellar developer
can deploy on their own infrastructure, point at their contracts, and get
back everything that happens to them: events and contract state, complete,
queryable, and honest about coverage.

```
1. Deploy the container next to an empty Postgres   (Railway button / compose / any cloud)
2. POST /v1/contracts {"contract_id": "C...", "from": "genesis"}
3. The indexer discovers the contract's events from its on-chain spec,
   backfills its full history, and follows the tip
4. GET /v1/contracts/C.../events?topic0=...&after=<cursor>
```

If the user has to touch code, the design has failed. Configuration is data:
boot config in env vars, hot config in the database, both managed through an
authenticated admin surface (API first; CLI and UI speak the same API).

### Positioning (why this and not the alternatives)

Every existing option is a hosted/paid service (Mercury, stellarindexer.com,
Obsrvr gateway), a toolkit you must assemble (nebu/flowctl, CDP building
blocks), or a framework you must fork and code (SubQuery). The turnkey
self-hosted appliance — clone, deploy, register, query — is vacant. Demand
for it is documented in the Stellar #indexers channel (7-day retention pain,
"I would pay for it", state-query pain).

**Durable differentiators** (getEvents v2 will eventually offer full event
history over RPC — see §8):
1. Your infrastructure, your data, ~$10/month, no vendor.
2. **Backward redefinition of scope**: register a contract today, get its
   complete past — including ranges no RPC serves anymore (archive replay).
3. **Contract state**, not just events — the resource event-indexers ignore
   and users demonstrably need (Blend case: 35 RPC calls → 1).
4. **Honesty as a feature**: declared coverage and gaps in every response.

### Non-goals (v1)

- No analytics/aggregations, no GraphQL, no dashboards over the data.
- No push delivery (webhooks/queues) — designed for, not built (§10).
- No multi-tenancy, no billing, no hosted offering.
- No chain-wide indexing: only registered contracts.
- No custom user code execution (no retroshades-style logic).

## 2. Decisions record

| # | Decision | Resolution |
|---|---|---|
| D1 | Data-type config granularity | Per contract, auto-classified default; overridable `kinds`. Config controls what is *derived/stored*, never what is *fetched* (ingestion is whole-ledger, filtering is local) |
| D2 | Internal data contract | Go structs; JSON only at the edges (store/API). No protobuf until an external-processor registry is a real need |
| D3 | Envelope & identity | `meta` + `payload` split; `schema` version in the datum; public id `{toid}-{event_index}`; idempotency key `network:contract_id:tx_hash:event_index` |
| D4 | API shape | `/v1/contracts/:id/{events,state}` (+`transfers` in v1.1); opaque cursor encoding the full query |
| D5 | v1 data resources | **Events + contract state**. SAC transfers/trustlines → v1.1 |
| D6 | Ecosystem coupling | Conventions yes, dependencies no. Single structural dependency: official `stellar/go-stellar-sdk` (ingest package) |
| D7 | getEvents v2 compat | v1 requirement: flat filters with `topic0..topic3`, opaque cursors, `scanStatus` semantics per stellar#1872 |
| D8 | State endpoint shape | Change history + simple current snapshot. Creit-style multi-contract batch query → v1.x |

## 3. Architecture

```
                ┌────────────────────────────────────────────────┐
                │                    sources                      │
                │   rpc (live+retention) · captive core (archive) │
                │   datastore/lake (S3/GCS/galexie)   [P1,P3,P5]  │
                └────────────────────┬───────────────────────────┘
                                     │ xdr.LedgerCloseMeta (whole ledgers, P2)
                ┌────────────────────▼───────────────────────────┐
                │                   ingest                        │
                │  live loop · backfill (desc chunks) · archive   │
                │  failover pool · tip watchdog · catch-up aware  │
                └────────────────────┬───────────────────────────┘
                                     │
                ┌────────────────────▼───────────────────────────┐
                │                  process                        │
                │  registry snapshot (per-ledger reload) filters  │
                │  extract: events · state changes    [P10–P13]   │
                │  per-ledger buffer, worker pool                 │
                └────────────────────┬───────────────────────────┘
                                     │ one transaction per ledger [P6]
                ┌────────────────────▼───────────────────────────┐
                │                   store (Postgres)              │
                │  raw + derived · cursor · gaps · coverage       │
                │  migrations owned by the app       [P14–P16]    │
                └────────────────────┬───────────────────────────┘
                                     │
      ┌──────────────────┬───────────┴──────────┬─────────────────────┐
      │   api (v1)       │   admin (authn)      │  observability       │
      │ events · state   │ contracts · replay   │ /health /ready       │
      │ cursor pull      │ reseed · rederive    │ /status /metrics     │
      │ [P17,P18]        │ [P21,P23]            │ [P19,P20]            │
      └──────────────────┴──────────────────────┴─────────────────────┘
```

Two runnable modes from one binary [Ponder pattern, P24]:
- `indexer run` — everything (default; the appliance).
- `indexer serve` — API only, no ingestion; for scaling reads separately.
- `indexer replay|rederive|reseed` — operational subcommands, run beside the
  live process, never inside it [P23].

### Package layout (Go)

```
cmd/indexer/            main; subcommand dispatch
internal/source/        LedgerSource interface + rpc/, captive/, datastore/
internal/ingest/        live loop, backfill, catch-up, failover, clamp
internal/registry/      registered contracts; spec classification; snapshot
internal/extract/       XDR → envelope structs (events, state changes)
internal/process/       buffer, worker pool, kind routing
internal/store/         Postgres; migrations; queries; cursor+gaps+coverage
internal/api/           public v1 handlers; cursor codec; getEvents-v2 compat
internal/admin/         authenticated admin API (registration, ops)
internal/health/        /health /ready /status; Prometheus metrics
internal/config/        boot config: load, validate, redact [P22]
```

Rules: interfaces are declared by consumers and stay small (1–3 methods)
[P26]; `internal/` everything until stability is promised — when a public
`pkg/` contract exists, its API is snapshot-tested in CI [nebu pattern, P27];
no CGO anywhere [P28]; every external call takes a context with a deadline
[P29].

### Core interfaces (sketch, consumer-side)

```go
// internal/source — the only seam ingestion sees. [P1]
type LedgerSource interface {
    // GetLedger blocks until the requested ledger is available or ctx ends.
    GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error)
    // Range reports what this source can serve *for getLedgers-equivalent
    // access* — authority is actual serving capability, never getHealth. [P3]
    Range(ctx context.Context) (oldest, latest uint32, err error)
    Close() error
}

// internal/process — extraction is pure: meta in, records out. [P12,P13]
type Extractor interface {
    Extract(lcm xdr.LedgerCloseMeta, watch registry.Snapshot) (Batch, error)
}

// internal/store — one commit per ledger; cursor and data are atomic. [P6]
type Committer interface {
    CommitLedger(ctx context.Context, seq uint32, hash string, b Batch) error
}
```

## 4. Data model

### Envelope (D3) — fixed before the first row is written

```jsonc
{
  "schema": "indexer.event.v1",          // version lives in the datum
  "meta": {
    "network": "mainnet",
    "ledger_sequence": 60200000,
    "closed_at": "2026-08-14T12:00:00Z", // business clock [P9]
    "tx_hash": "7361f5…",
    "tx_index": 1, "op_index": 0, "event_index": 0,
    "contract_id": "C…"
  },
  "payload": { /* decoded topics + data; raw XDR kept in store */ }
}
```

- Public id: `{toid}-{event_index}` (sortable, Horizon-convention).
- Idempotency key (unique index): `network:contract_id:tx_hash:event_index`.
- `ingested_at` exists in every table and is **never** used in business
  queries or sorts [P9 — the read-model-traps lesson].

### Tables (owned schema, auto-migrated [P14])

- `contracts` — id, source (config|api), kinds, registered_at; config-seeded
  rows win over API rows [Umbra].
- `events` — raw XDR + decoded payload (JSONB), envelope meta columns,
  unique idempotency key.
- `state_entries` — current snapshot: (contract_id, key_xdr) → value, last
  modified ledger. Cheap upsert per change [D8].
- `state_changes` — history: every entry change with provenance.
- `cursor` — singleton per network; written in the same tx as data [P6].
- `gaps` — first-class, deterministic id, persisted before processing,
  resolved explicitly [P7].
- `coverage` — per contract: covered ranges, backfill progress by chunk
  [Umbra pattern].
- `ledger_hashes` — recent chain continuity (PreviousLedgerHash check on
  resume; testnet-reset detection) [P8].

Retention: configurable pruning of raw XDR / old state history; safe because
everything is re-materializable from chain + archives [P16]. Derived tables
can always be rebuilt from raw via `rederive` [P13].

## 5. Ingestion

- **Live loop**: single writer. Pulls whole ledgers from the active source,
  filters against the registry snapshot (reloaded per ledger — marginal cost
  zero [P2]), extracts, commits atomically, advances cursor.
- **Failover pool** across RPC endpoints: per-attempt deadlines, window-error
  classification (beyond-tip waits; below-retention fails fast), tip-stall
  watchdog. Serving authority = getLedgers behavior, not getHealth [P3].
- **Backfill**: descending chunks (recent history first), progress persisted
  per chunk, interruption-safe, clamps at the retention wall with one honest
  gap [Umbra]. Runs in background workers, bounded pool [P4].
- **Archive leg** (v1.1, designed now): captive core replaying History
  Archives for ranges below RPC retention. Non-negotiable: replay config
  must match RPC event semantics (`EmitUnifiedEvents` + BeforeProtocol22),
  gated by a byte-equality test against an RPC-served range before trust
  [P5]. Ships as the `-full` image variant (needs stellar-core + disk).
- **Catch-up awareness**: no tip-state stamping while behind [TW lesson 6].
- Transient failures back off; they never exit the process [P24].

## 6. Processing

- Per-ledger **buffer** accumulates all extracted records, then one commit
  [P11]. Parallelism inside extraction (worker pool), serialization at the
  commit [P4].
- **Systematic distrust** [P12]: check `Result.Successful()` on every tx
  (SDK streams failed txs too); `recover` frontier around SDK XDR getters
  (they panic on nil); origin validation on SAC-style events
  (topics contract == emitting contract); mass-absence plausibility guard
  before deriving any removal, with a suppression counter exposed as a
  metric [P20].
- **Auto-classification** on registration: read the contract's on-chain
  spec (`contractspecv0`) → discover event names (normalize CamelCase→
  snake_case), detect SAC by executable; fallback to function names. Result
  is stored, reported in the API, and overridable [D1].

## 7. Configuration (D1, P21)

**Boot (env, restart to change):**
```
DATABASE_URL=postgres://…      # empty database; the app owns the schema
NETWORK=mainnet|testnet
ADMIN_TOKEN=…                  # min-entropy enforced at boot
RPC_URLS=…                     # comma-separated failover pool (default: public)
```
Config prints redacted; unknown/dead variables are a startup error [P22].

**Hot (in the database, survives redeploys, changed via admin API):**
contract registrations and their `kinds`, backfill origin (`genesis` |
ledger | date), retention/pruning windows.

## 8. Public API (v1)

Design rule (D7): the events surface is a **compatible superset of the
getEvents v2 proposal** (stellar#1872). Same filter semantics, same cursor
philosophy, same scan-status honesty. When v2 ships, this server becomes its
self-hosted full-history implementation, and a byte-compatible RPC facade
(the Umbra drop-in play) is a thin adapter.

- `GET /v1/contracts/:id/events`
  - Filters: `topic0..topic3` (exact ScVal match, omitted = wildcard, AND
    within a filter), `type`, time/ledger range params.
  - Pagination: `limit` + opaque `cursor` that encodes the *entire query*
    (bounds, order, filters); cursors don't expire and honor original
    bounds.
  - Response: `records[]`, `cursor`, `scanStatus`
    (`HAS_MORE|WAITING_FOR_LEDGERS|OLDEST_REACHED|COMPLETE`), and
    `coverage` for the requested range — a page can be empty because there
    is nothing OR because we haven't indexed it; the response always says
    which [P7, #1872].
- `GET /v1/contracts/:id/state` — current snapshot (`?key=` filter).
- `GET /v1/contracts/:id/state/history` — entry changes, same pagination.
- `GET /v1/contracts/:id` — registration, classification, coverage, counts.
- `GET /v1/status` — instance-wide: network, cursor, tip lag, per-contract
  coverage, open gaps, suppressed-removal counters.
- Admin (Bearer `ADMIN_TOKEN`): `POST/DELETE /v1/contracts`,
  `POST /v1/admin/replay`, `/reseed`, `/rederive`.
  Admin follows the control-plane doctrine: pull, not push; reconcile sets,
  not events; every mutation idempotent [TW A1].

## 9. Observability (P19, P20)

- `/health` — process up (200 immediately).
- `/ready` — 200 only at tip; 503 during backfill/catch-up [Ponder].
- `/metrics` — Prometheus: ledgers ingested, tip lag, per-source failovers,
  backfill progress, commit latency, suppressed removals, open gaps, DLQ-less
  by design but every drop path counted. Grafana dashboard JSON ships in
  `deploy/grafana/`.
- `/status` — human-readable JSON (the dashboard's data source).
- A public status page (Gatus config in `deploy/`) is part of the deliverable:
  in this ecosystem, observability is marketing.

## 10. Delivery roadmap (designed now, built later)

v1 delivery is **pull with cursor** — the only exactly-once mode that asks
nothing of the consumer [P17]. The store is the log; every future push
transport is a consumer of that log with its own cursor:
- v2: signed webhooks (at-least-once, retries+backoff, per-hook cursor).
- v2: broker sinks (RabbitMQ/NATS) for teams that already have one.
- The TW repo's `docs/pluggable-sink-architecture.md` is the seed research.

## 11. Distribution

- **One Docker image** (`ghcr.io/...:semver`), distroless/static, no CGO
  [P28]. `-full` variant adds stellar-core for the archive leg.
- **Railway template** (app + Postgres wired) — the front-door button.
- **docker-compose.yml** — local and VPS path.
- **Static binaries** per platform on GitHub releases.
- Runs identically on Railway / AWS (ECS+RDS) / GCP (Cloud Run+Cloud SQL) /
  any VPS: it's a container plus a `DATABASE_URL`.

## 12. Quality gates (the bar Caleb set)

- CI: gofmt, go vet, staticcheck, gosec, govulncheck, `go test -race`,
  integration tests against a real testnet range (recorded fixtures for
  determinism).
- Coverage of the distrust paths is mandatory: failed-tx filtering, panic
  frontier, plausibility guard, gap persistence, resume-with-hash-check.
- Conventional commits, semver, CHANGELOG, signed releases.
- Docs are part of the definition of done: every endpoint in OpenAPI, every
  metric documented, a runbook per operational subcommand.
- If a public `pkg/` is ever promised stable: API snapshots enforced in CI
  [nebu].

## 13. Milestones

- **M0 — skeleton**: repo, CI, config, health server, Postgres migrations,
  cursor loop against RPC with continuity checks. No extraction yet.
- **M1 — events end-to-end**: registration + classification, live events,
  descending backfill within retention, coverage/gaps, events API with
  v2-compatible filters. *First demoable release.*
- **M2 — state**: state_changes + snapshot, state API.
- **M3 — appliance polish**: Railway template, Grafana dashboard, status
  page, docs site, versioned release. *Public v1.*
- **v1.1** — archive leg (`-full` image), SAC transfers/trustlines.
- **v1.x** — Creit-style batch state query; getEvents v2 facade when it
  ships.
- **v2** — webhooks, broker sinks, management UI.
