---
name: live-smoke
description: Run and interpret an end-to-end smoke of Sierpe against real testnet with a throwaway Postgres in Docker — the verification every PR that touches ingest, source/rpc, registry, extract, store or api needs before merge. Use when asked to "verify live", "smoke", "prove it works against testnet", or before merging any pipeline change.
---

# Live smoke against testnet

Unit tests with fakes are not enough for pipeline changes: both RPC races found so
far only showed up live. A smoke costs ~5 minutes and has caught a fatal-at-tip
bug twice. Do it before proposing the merge of anything under `internal/{ingest,
source,registry,extract,store,api}`.

## Setup (the user runs Docker; you prepare the commands)

1. Throwaway Postgres on a non-default port so nothing collides:
   ```bash
   docker run -d --name sierpe-smoke -p 5544:5432 -e POSTGRES_PASSWORD=smoke -e POSTGRES_DB=sierpe postgres:16
   ```
2. **Truncate / start empty.** If reusing a DB that ran the test suite, a residual
   cursor (e.g. 601) makes a fresh boot look like legitimate below-retention and the
   process exits. Fresh container = safest.
3. Env: `DATABASE_URL=postgres://postgres:smoke@localhost:5544/sierpe`,
   `NETWORK=testnet`, `ADMIN_TOKEN=<32+ random chars>` (entropy is validated at boot).
4. `go build -o /tmp/sierpe ./cmd/sierpe && /tmp/sierpe run` (or the Docker image
   when the change is in the Dockerfile).

## What to check, in order (each maps to a rule in CLAUDE.md)

- **Boot log**: config printed with secrets REDACTED (rule 12); migrations applied;
  "starting at tip N" on a fresh DB.
- **Live ingestion**: commits every ledger (~4 ms each); `/health` 200, `/ready`
  200 once at tip (503 during catch-up), `/status` cursor advancing, `/metrics`
  present.
- **Chain integrity in DB**: `select count(*) from ledger_hashes l join ledger_hashes p
  on l.previous_hash = p.hash` equals rows−1 (rule 1/6).
- **Resume**: kill, restart → "resuming from cursor" and the FIRST ledger after
  resume verifies continuity.
- **Sabotage** (only when touching ingest/store): corrupt `cursor.last_hash` by hand →
  exit 1 with "chain divergence" and ZERO new writes.
- **Registration + classification**: `POST /v1/contracts` with a known testnet
  contract (an OZ token → `wasm/spec_events` with mined event names; XLM SAC →
  `sac_builtin`; a fake id → 404).
- **Backfill**: descending chunks, watermark advancing, gap persisted BEFORE the
  retention clamp if the wall is hit; `scanStatus` reaches `OLDEST_REACHED` or
  `COMPLETE`.
- **API**: `topic0` filter narrows; page 2 via cursor has no overlap; cursor + explicit
  params → 400; `coverage` declared on every response; `/state` snapshot vs
  `/state/history` count (history ≥ live entries).
- **Soak at tip ≥ 4 minutes** when touching source/rpc: the two known races
  (`getLatestLedger` ahead of `getLedgers`; failed tip probe) must NOT exit the
  process — they log as transient and back off.

## Teardown

`docker rm -f sierpe-smoke`; delete the scratch binary. Record in the PR body what
was verified live (the milestone PRs have the format).

## Done when

Every applicable check above is listed in the PR body with its observed value; any
new failure mode found becomes a regression test before merge.
