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
| `sierpe_state_changes_extracted_total` | counter | Contract-data changes from watched contracts committed to the store. | Zero while state-kind contracts are active = extraction problem. |
| `sierpe_transfers_extracted_total` | counter | Token transfers decoded from watched contracts and committed to the store. | Zero while transfers-kind contracts are active = decoder problem. |
| `sierpe_trustline_changes_extracted_total` | counter | Classic trustline changes of watched SAC assets committed to the store. | Progress signal for trustlines-kind registrations. |
| `sierpe_failed_txs_skipped_total` | counter | Failed transactions skipped during extraction; their events never happened. | Routine traffic; no alert. |
| `sierpe_suppressed_txs_total` | counter | Transactions dropped because their meta was unreadable or panicked mid-decode. | **Alert if nonzero** — this is counted data loss. |
| `sierpe_suppressed_events_total` | counter | Events dropped because their XDR could not be re-encoded. | **Alert if nonzero** — same semantics. |
| `sierpe_suppressed_transfers_total` | counter | Events that named a token movement but did not decode as one; the raw event row still lands. | **Alert if nonzero** — the decoder no longer matches what the network emits. |
| `sierpe_suppressed_trustlines_total` | counter | Watched trustline changes that could not be read. | **Alert if nonzero** — counted data loss. |
| `sierpe_backfill_chunks_total` | counter | Backfill chunks committed since process start. | Stalls while `sierpe_backfill_pending` > 0 = stuck worker. |
| `sierpe_backfill_ledgers_scanned_total` | counter | Ledgers covered by committed backfill chunks. | Progress rate of history walks. |
| `sierpe_backfill_pending` | gauge | Registered contracts whose backfill has not finished. | Should drain to 0 after registrations. |

The suppression counters exist because of CLAUDE.md rule 6: a silent guard
turns an invisible failure into another invisible failure. Anything Sierpe
refuses to store is counted where an operator can see it.
