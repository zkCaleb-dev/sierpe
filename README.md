# Sierpe

> **Status: v1.1.0.** The design is stable
> ([docs/DESIGN.md](docs/DESIGN.md)) and the v1 surface (events + contract
> state) is feature-complete and verified live against testnet. Expect
> pre-1.x-maturity rough edges; issues and feedback are welcome.

**Your own Stellar indexer, deployed in minutes.**

Sierpe is a self-hosted server that watches the Stellar network for the
contracts *you* register and keeps their complete history — events,
contract state, decoded token transfers, and the classic trustlines of
SAC assets — in your own Postgres, behind an honest REST API.

```
1. Deploy the container next to an empty Postgres
2. POST /v1/contracts {"contract_id": "C...", "from": "genesis"}
3. Sierpe discovers the contract's events from its on-chain spec and
   backfills its full history — including ranges no RPC serves anymore
4. GET /v1/contracts/C.../events?topic0=...&after=<cursor>
```

No forks, no custom code, no vendor. Configuration is data: register
contracts at runtime through an authenticated API, and the indexer classifies
them, backfills them, and follows the tip.

## Why Sierpe

- **Your infrastructure, your data.** A container plus a `DATABASE_URL`.
  Runs the same on Railway, AWS, GCP, or a $5 VPS. Target cost for a typical
  project: under $10/month.
- **History past the RPC window.** Stellar RPCs retain ~7 days of events.
  Sierpe backfills from genesis where sources allow, and is designed to
  replay History Archives for ranges no RPC serves at all.
- **Contract state, not just events.** Storage entry changes and current
  snapshots — the data most event indexers ignore.
- **Honest by construction.** Coverage and gaps are first-class data,
  declared in every API response. An empty page always tells you whether
  there is nothing — or whether we haven't indexed it yet.
- **Forward-compatible.** The events API follows the semantics of the
  proposed `getEvents` v2 RPC endpoint (filters, opaque cursors, scan
  status), so integrations built against Sierpe speak tomorrow's standard.

## What Sierpe is not

- Not a hosted service — you run it (that's the point).
- Not an analytics platform: no aggregations, no dashboards over your data.
- Not a chain-wide indexer: it indexes the contracts you register.
- Not a framework: if you have to write code to use it, that's a bug.

## Documentation

- [docs/DESIGN.md](docs/DESIGN.md) — architecture, data model, API surface,
  configuration, milestones.
- [docs/DEPLOY.md](docs/DEPLOY.md) — Railway, Docker Compose, and generic
  container deployments.
- [docs/KNOWLEDGE.md](docs/KNOWLEDGE.md) — the study behind the design:
  29 principles distilled from production indexers, each with its source.
- [CONTRIBUTING.md](CONTRIBUTING.md) — how to build, test, and propose
  changes.
- [SECURITY.md](SECURITY.md) — how to report vulnerabilities.

## Quickstart (M2: events and state end-to-end)

Requirements: Docker (or Go 1.25+) and an empty Postgres — see [docs/DEPLOY.md](docs/DEPLOY.md).

```bash
export DATABASE_URL=postgres://user:pass@localhost:5432/sierpe
export NETWORK=testnet
export ADMIN_TOKEN=change-me-to-something-long
go run ./cmd/sierpe run
```

Register a contract — Sierpe classifies it from its on-chain spec, walks
its history backwards in chunks, and follows the tip:

```bash
curl -X POST localhost:8080/v1/contracts \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"contract_id": "C...", "from": "genesis"}'
```

Query its events with getEvents-v2-style filters, its current storage
snapshot, or the full change history of any entry — all with honest paging:

```bash
curl "localhost:8080/v1/contracts/C.../events?topic0=<base64-scval>&limit=100"
curl "localhost:8080/v1/contracts/C.../state?key=<base64-scval>"
curl "localhost:8080/v1/contracts/C.../state/history?startLedger=..."
```

Every response declares `coverage` and a `scanStatus`
(`HAS_MORE | WAITING_FOR_LEDGERS | OLDEST_REACHED | COMPLETE`), and the
opaque `cursor` encodes the whole query, so pagination never drifts. The
full surface is specified in [docs/openapi.yaml](docs/openapi.yaml); metrics
are documented in [docs/METRICS.md](docs/METRICS.md).

## Roadmap

| Milestone | Contents |
|---|---|
| M0 ✅ | Skeleton: config, health, migrations, cursor loop with continuity checks |
| M1 ✅ | Events end-to-end: registration, classification, live + backfill, events API |
| M2 ✅ | Contract state: change history + current snapshot |
| M3 ✅ | Appliance polish: container, compose, Grafana dashboard, status page, docs — released as v1.0.0 |
| v1.1 ✅ | Token transfers (SEP-41 decode, CAP-67 muxed) and classic trustlines of SAC assets |
| v1.2 | Archive replay: history below RPC retention (captive core, `-full` image) |
| v2 | Push delivery: signed webhooks, broker sinks, management UI |

## License

[Apache-2.0](LICENSE)
