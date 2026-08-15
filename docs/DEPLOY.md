# Deploying Sierpe

Sierpe is a container plus a `DATABASE_URL`. Every path below is the same
appliance; pick the one that matches your infrastructure.

## Configuration (all deployments)

| Variable | Required | Meaning |
|---|---|---|
| `DATABASE_URL` | yes | Postgres connection string. Give Sierpe an **empty database**: it owns the schema and runs its own migrations. |
| `NETWORK` | yes | `testnet` or `mainnet`. |
| `ADMIN_TOKEN` | yes | Bearer token for the admin API. Minimum entropy enforced at boot. |
| `RPC_URLS` | mainnet | Comma-separated failover pool of Stellar RPC endpoints, in preference order. Testnet defaults to the public SDF endpoint. |
| `HTTP_PORT` | no | API port, default 8080. |
| `START_LEDGER` | no | First ledger for a fresh database (default: current tip). |

Operational truths that apply everywhere:

- **Postgres proximity matters.** Keep app and database in the same region
  or host; latency above ~50ms degrades commit throughput.
- **`/ready` gates traffic** (503 while catching up); `/health` is liveness.
- **Crash recovery is the default**: same database means the cursor resumes
  and continuity is verified against the stored hash chain. No manual steps.
- Your backend consumes the HTTP API. The tables are private to Sierpe.

## Docker Compose (local, VPS)

```bash
export POSTGRES_PASSWORD=$(openssl rand -hex 16)
export ADMIN_TOKEN=$(openssl rand -hex 24)
docker compose up -d
curl localhost:8080/health
```

The bundled [docker-compose.yml](../docker-compose.yml) wires Sierpe to its
own Postgres 16 with a named volume. Set `NETWORK=mainnet` and `RPC_URLS`
for mainnet.

## Railway

1. Create a project with a **Postgres** service.
2. Add a service from this GitHub repo — Railway builds the Dockerfile.
3. Set the variables: `DATABASE_URL` = `${{Postgres.DATABASE_URL}}`,
   `NETWORK`, `ADMIN_TOKEN` (and `RPC_URLS` on mainnet).
4. Expose the service; `/health` is the health check path.

Target footprint for a typical project (a handful of contracts) fits
Railway's smallest paid tier: the volume is proportional to your contracts'
activity, not to the chain.

## AWS / GCP / anything with containers

ECS + RDS, Cloud Run + Cloud SQL, a Nomad job next to a Postgres — the shape
is always: run the image, point `DATABASE_URL` at an empty database, put the
HTTP port behind your ingress, gate on `/ready`.

## First contract

```bash
curl -X POST https://your-instance/v1/contracts \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"contract_id": "C...", "from": "genesis"}'
```

Sierpe classifies the contract from its on-chain spec, walks its history
backwards in atomic chunks (clamping honestly at the RPC retention wall),
and follows the tip. Watch progress in `/status` (`pending_backfills`) or
the `sierpe_backfill_*` metrics.

## Observability

`/metrics` is Prometheus-ready; every metric is documented in
[METRICS.md](METRICS.md). A ready-made Grafana dashboard and a Gatus status
page config ship in [deploy/](../deploy/).
