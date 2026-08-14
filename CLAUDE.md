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
- Branch from `main`; PRs must pass CI.

## Language

Code, comments, docs, and commit messages are in **English** (public OSS
project). Conversation with the maintainer may be in Spanish.
