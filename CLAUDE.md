# CLAUDE.md — rules of this repository

Sierpe (working name) is a self-hosted Stellar contract indexer: a **server**
in the Postgres/Prometheus sense. Deploy it next to an empty Postgres, register
contracts, get their complete event and state history through an honest API.

Read `docs/DESIGN.md` before writing code. `docs/KNOWLEDGE.md` holds the 29
principles (P1–P29) this project is built on, each with its source. Design
decisions D1–D8 are recorded in DESIGN.md §2 — do not re-litigate them in code
review or refactors; open a discussion first if one seems wrong.

## Architecture — must be respected

The pipeline is: **source → ingest → process → store → serve**, with
state (cursor, gaps, coverage) crossing all stages.

```
cmd/sierpe/          main; subcommand dispatch (run | serve | replay | rederive | reseed)
internal/source/     LedgerSource interface + rpc/, captive/, datastore/ implementations
internal/ingest/     live loop, backfill, catch-up, failover, retention clamp
internal/registry/   registered contracts; on-chain spec classification; atomic snapshot
internal/extract/    XDR → envelope structs (pure functions: meta in, records out)
internal/process/    per-ledger buffer, worker pool, kind routing
internal/store/      Postgres: migrations, queries, cursor+gaps+coverage
internal/api/        public v1 handlers; opaque cursor codec; getEvents-v2-compatible filters
internal/admin/      authenticated admin API (registration, replay, reseed, rederive)
internal/health/     /health /ready /status; Prometheus metrics
internal/config/     boot config: load, validate, redact
```

Hard rules:

1. **Cursor and data commit in the same Postgres transaction.** Exactly-once
   lives or dies here. Never split them.
2. **Ingestion downloads whole ledgers and filters locally.** Registering a
   contract must cost zero extra RPC. Config controls what is *derived and
   stored*, never what is *fetched*.
3. **The only structural dependency is the official `stellar/go-stellar-sdk`.**
   Adopt ecosystem conventions (envelope shapes, filter semantics), never
   pre-1.0 third-party dependencies.
4. **No CGO.** The binary must stay static and cross-compilable.
5. **Interfaces are declared by their consumer and stay small** (1–3 methods).
   No speculative abstraction.
6. **Distrust the chain data systematically**: check `Result.Successful()` on
   every transaction (the SDK streams failed ones too); wrap SDK XDR getters
   in a recover frontier (they panic on nil); never derive a removal from an
   absence without the plausibility guard; count everything suppressed in a
   metric.
7. **Gaps and coverage are data, not logs.** Persist a gap before processing
   around it; declare coverage in API responses. A range we cannot vouch for
   is stated, never implied.
8. **Two clocks, never mixed**: `closed_at` (ledger time) is the business
   clock; `ingested_at` is operational. Business queries and sorts use
   `closed_at` only.
9. **Serving authority is real `getLedgers` behavior, not `getHealth`.**
   Window errors classify as beyond-tip (wait) vs below-retention (fail fast).
10. **Transient failures back off; they never exit the process.** Fatal is
    reserved for integrity violations (hash divergence, network mismatch).
11. **Admin plane doctrine**: pull, not push; reconcile sets, not events;
    every mutation idempotent; always authenticated.
12. **Config that lies is a bug**: no dead env vars, secrets redacted in any
    config printout, validation at boot with actionable errors.

## Where the project stands (update this block when it changes)

- **Feature-complete M0–M3, merged on `main`** (2026-08-15): ingestion with
  hash-chain continuity, registry + classification, backfill with retention
  wall, events + state APIs (getEvents-v2 compatible), distroless image,
  compose, Grafana/Gatus configs, goreleaser. All verified live against testnet.
- **v1.0.0 released (2026-08-20) under the final name `sierpe`.** The tag
  froze the module path (`github.com/zkCaleb-dev/sierpe`), the image name
  (`ghcr.io/zkcaleb-dev/sierpe`) and the v1 API surface. Releases follow
  `docs/RELEASING.md` (manual goreleaser + docker push).
- **v1.1.0 released (2026-08-20): token transfers + classic trustlines.**
  Two new data kinds: `transfers` (SEP-41 movement decode, CAP-67 muxed
  ids, SAC default) and `trustlines` (opt-in; asset hashed locally to its
  SAC, holder snapshot with tombstones), each with its own read endpoint
  and endpoint-bound cursors.
- **GitHub Actions is locked** (billing) and the maintainer will not pay for it.
  The real gate is LOCAL: gofmt, build, vet, `test -race` with a throwaway
  Postgres, staticcheck. Goreleaser runs manually. Do not propose paying.
- Roadmap: archive leg (captive core, `-full` image) is v1.2. Railway
  template still needs the maintainer's account.

## Product decisions (do not re-litigate; see DESIGN.md for the why)

- It is a **server** (Postgres/Prometheus category), not a framework and not a
  fork-and-edit template. If the user has to touch code, the design is wrong.
- Owns its Postgres schema; consumers read through the API, never the tables.
- Target user cost < $10/month; volume scales with registered contracts.
- Pull with cursor is THE delivery (exactly-once); queues/webhooks are v2 on top.
- Differentiators: redefine "what concerns me" backwards in time (full history
  even beyond RPC retention), and data honesty (gaps, coverage, scanStatus).

## Decisions taken while building (M1–M3) — behaviour, not accidents

- `DELETE /v1/contracts/:id` keeps indexed data; re-registering resumes.
- Unreadable spec degrades the contract to `opaque` without failing registration;
  a contract that does not exist on-chain rejects the POST (404).
- Coverage is DERIVED from backfill watermark + cursor; there is no coverage table.
- Registering before any cursor exists → backfill is done-at-birth with a warning.
- Contracts whose instance is ARCHIVED (TTL expired) 404 on registration — known
  limit; archived detection + restore is future work.
- Cursors carry their `kind`; a cursor from `/events` is invalid on `/state`.

## Live RPC traps (both cost a smoke run; both have regression tests)

- `getLatestLedger` may announce a ledger that `getLedgers` does not serve yet.
  A window error within 64 ledgers of the reported tip is `NotYetAvailable`,
  never below-retention. Do not "simplify" that classification.
- A failed tip probe during a window error is transient, not below-retention.
- A test run leaves a residual cursor in the test database; **truncate everything
  before a live smoke** or the fresh boot looks like a legitimate below-retention.

## Verification

- `go build ./... && go vet ./... && go test -race ./...` must pass before any
  commit is proposed.
- gofmt everything; CI also runs staticcheck, gosec, govulncheck.
- New distrust paths (rule 6) require tests. New endpoints require OpenAPI
  updates. New metrics require documentation.

## Git conventions

- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`…), imperative mood.
- Commit messages contain no quotes, apostrophes, or backticks.
- **No AI co-authorship trailers of any kind** (no Co-Authored-By for Claude
  or any assistant). This is a hard project rule.
- Branch from `main` (fetch first; branch from `origin/main`); land through a
  GitHub PR + merge. The maintainer merges unless they explicitly delegate it.
- Definition of done = the local gate above + docs updated (OpenAPI for
  endpoints, METRICS.md for metrics, CHANGELOG `[Unreleased]` for anything
  user-visible). Docs are part of done (DESIGN.md §12).
- The closing package (commit / PR / merge texts) and cross-repo rules come from
  the `caleb-workflow` plugin (`finish-work`, `workflow-rules`, push-to-main
  hook) — `github.com/zkCaleb-dev/claude-plugins`. Repo procedures live in
  `.claude/skills/`: `live-smoke`, `api-surface-change`.

## Language

Code, comments, docs, and commit messages are in **English** (public OSS
project). Conversation with the maintainer may be in Spanish.
