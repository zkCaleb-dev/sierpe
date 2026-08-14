# Metrics

Every Prometheus metric Sierpe exposes at `/metrics`, in one place. A new
metric does not ship without a row here (CLAUDE.md verification rules).

| Metric | Type | Meaning | Watch for |
|---|---|---|---|
| `sierpe_ledgers_ingested_total` | counter | Ledgers committed since process start. | Stalls: rate ≈ 1 per 5s at the tip. |
| `sierpe_tip_lag_seconds` | gauge | Age of the last committed ledger vs wall clock. | Sustained growth = falling behind. |
| `sierpe_source_failovers_total` | counter | Times the RPC pool switched endpoints. | Bursts = unhealthy preferred endpoint. |
| `sierpe_commit_duration_seconds` | histogram | Time to commit one ledger (cursor + continuity + events, one transaction). | p99 growth = database pressure. |
| `sierpe_open_gaps` | gauge | Unresolved coverage gaps recorded in the database. | Any nonzero value is declared, unserved history. |
| `sierpe_events_extracted_total` | counter | Events from watched contracts committed to the store. | Zero while contracts are active = extraction problem. |
| `sierpe_failed_txs_skipped_total` | counter | Failed transactions skipped during extraction; their events never happened. | Routine traffic; no alert. |
| `sierpe_suppressed_txs_total` | counter | Transactions dropped because their meta was unreadable or panicked mid-decode. | **Alert if nonzero** — this is counted data loss. |
| `sierpe_suppressed_events_total` | counter | Events dropped because their XDR could not be re-encoded. | **Alert if nonzero** — same semantics. |

The suppression counters exist because of CLAUDE.md rule 6: a silent guard
turns an invisible failure into another invisible failure. Anything Sierpe
refuses to store is counted where an operator can see it.
