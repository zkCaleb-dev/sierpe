# Railway template — maintainer runbook

The template is the one-click front door (DESIGN §11). Railway templates
are created and edited in the dashboard (there is no CLI/API path), so
this runbook records every field, so any maintainer can create or update
it identically.

## Composition (railway.com → New → Template, or Workspace → Templates)

Two services:

### Service 1: `sierpe`

| Field | Value |
|---|---|
| Source | Docker Image: `ghcr.io/zkcaleb-dev/sierpe:v1.4.2` |
| Healthcheck path | `/health` |
| Public networking | enabled, port `8080` |

Pin a version tag, never `latest`: a template deploy must be
reproducible, and users redeploy at unpredictable times. Bump the pin as
part of each release (see RELEASING.md).

Variables:

| Name | Value | Notes |
|---|---|---|
| `DATABASE_URL` | `${{Postgres.DATABASE_URL}}` | reference to service 2 |
| `NETWORK` | `testnet` | user-editable at deploy time; describe as "testnet or mainnet (mainnet also needs RPC_URLS)" |
| `ADMIN_TOKEN` | `${{secret(48)}}` | generated per deployment; gates registrations |
| `HTTP_BASIC_AUTH` | `sierpe:${{secret(24)}}` | generated per deployment; the whole surface (UI included) requires these credentials, so a fresh instance is private from minute zero |

### Service 2: `Postgres`

Add Railway's own **PostgreSQL** database service, unchanged. Sierpe owns
the schema inside it and runs its own migrations.

## Template description (paste as-is)

> **Sierpe — your own Stellar indexer, deployed in minutes.**
>
> Register a Soroban contract and get its complete history — events,
> contract state, decoded token transfers, classic trustlines — in your
> own Postgres, behind an honest REST API and a built-in management UI.
>
> After deploying: open the service URL, sign in with the generated
> `HTTP_BASIC_AUTH` credentials (Variables tab), paste your
> `ADMIN_TOKEN` in the UI's admin box, and register your first contract.
> Sierpe classifies it from its on-chain spec and backfills its history
> automatically.
>
> Runs on testnet out of the box. For mainnet, set `NETWORK=mainnet` and
> provide `RPC_URLS` (there is no free public mainnet RPC).

## After publishing

- The template page yields a deploy URL (`railway.com/deploy/<slug>`)
  and a "Deploy on Railway" button snippet: add both to README.md and to
  the project site.
- Test the template end to end from a CLEAN deployment before announcing:
  deploy, sign in with the generated credentials, register a contract,
  see data arrive. The template's first impression is the product's.

## Keeping it current

Every release (RELEASING.md): after images are pushed and verified, edit
the template's image pin to the new tag. Template edits do not affect
already-deployed instances — users upgrade by editing their own service
image, which is worth a line in release notes.
