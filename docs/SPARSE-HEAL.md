# Sparse heal — replaying only where the registered contracts lived

Status: **specification**, not implemented. Written against the measurements
of the first deep mainnet heal (September 2026).

The archive leg heals a gap by replaying every ledger in it through a captive
core. That is the only way to regenerate transaction meta — history archives
publish envelopes and results, never the meta where Soroban events and ledger
entry changes live — and it is correct. It is also, for a deep gap, slow
enough to be unusable. This document specifies how a heal skips the stretches
where no registered contract could have produced anything, without ever
claiming to have looked at them.

## 1. The measurements this design answers to

From the TrustlessWork mainnet pilot, healing a 6.21M-ledger gap on a Ryzen
5 3600 with a ~10 Mbps link:

| Quantity | Measured |
|---|---|
| Replay throughput | **4.72 ledgers/s** end-to-end (~4.8–5.0 net of spin-up) |
| Captive core spin-up | **13 min** for a deep chunk; **~1 h** for the first run of all, on cold scratch near the tip |
| Peak RAM per captive core | **8.9–9.8 GB, whether the chunk is 9 ledgers or 500,000** |
| Peak scratch | 47 GB |
| Ledgers with any activity of the 1,285 registered contracts | **9,222 of 6.21M — 0.15%** |
| Harvest of one 500k-ledger chunk | 13 events, 13 state changes, 3 movements |

Linear replay of the remaining 5.70M ledgers is ~14 days on one worker. The
data actually being recovered lives in 0.15% of them.

Two structural facts follow, and the design is shaped by both:

- **Spin-up is a fixed cost per replayed segment.** Cutting a range into more
  segments buys fewer replayed ledgers at the price of another spin-up.
  Splitting at a hole pays only when the hole is longer than
  `spin_up x throughput`, so the break-even moves with the spin-up: about
  **3,700 ledgers** at the 13-minute figure a deep chunk actually costs, and
  about **17,000** at the hour the very first run took. Measure your own
  before choosing a clustering threshold; the pilot planned at 10,000 on the
  pessimistic number and the curve is flat enough that it cost little.
- **Random per-ledger access would dissolve the problem**, and is not
  available. The SDF ledger-close-meta lake is not anonymously readable
  (verified empirically: `gs://sdf-ledger-close-meta` denies both
  `buckets.get` and `objects.list`; the other GCS and S3 candidates 404 or
  403). The public history archives at `history.stellar.org` are free and
  are the only usable source of deep history, so the design stays on captive
  core replay.

Clustering the 9,222 active ledgers with a 10,000-ledger hole threshold gives
57 clusters covering 565,846 ledgers: **57 spin-ups plus 33 h of replay, about
90 h**, against 335 h linear. Wider thresholds are worse (50k → 99 h, 100k →
113 h), which is what the 17,000-ledger break-even predicts.

The plan actually computed for the pilot's 1,285 contracts — event ledgers
unioned with deployment ledgers (§3), clustered at 10k with 300 ledgers of
padding on each side — comes to **58 intervals over 649,788 ledgers, 10% of
the range**: about 96 h on one worker, **48 h on two**. Padding accounts for
34,800 of those ledgers and costs roughly two hours, which is the right price
for the edge cases it absorbs.

## 2. The core idea: split the gap, never attest to it

A gap is a recorded promise that a range is missing. Today the healer walks
one gap downward to its floor. Sparse heal **splits** it instead:

```
gap 58,000,000 → 63,698,263            (one promise, walked linearly)

becomes

gap 58,000,000 → 59,146,454   deferred  (no known activity)
gap 59,146,455 → 59,152,703   replay    (cluster)
gap 59,161,088 → 59,161,215   deferred
...                                      57 replay gaps, 58 deferred gaps
```

Every ledger of the original range still belongs to exactly one open gap, so
nothing becomes silently unaccounted for (P7, rule 7). The healer replays the
`replay` gaps and leaves the `deferred` ones open and declared. Each cluster
carries its own `heal_next_to`, so **no watermark ever has to descend through
a stretch that was not replayed** — which was the whole obstacle.

### Why not attested coverage

The tempting alternative is to let the activity hint certify the deserts and
declare their coverage as `attested` rather than `replayed`. It is rejected.

Under split-gap the hint is **pure scheduling**: if it is wrong, some range
that deserved replay stays an open gap, and the instance under-claims. Under
attestation the same wrong hint becomes a false statement about indexed
history. Sierpe's differentiator is that a range it cannot vouch for is
stated and never implied; buying three days with an asterisk on that promise
is the one trade this project should not make.

The accepted cost is narrow but it has a sharp edge, and the edge is an
operational rule rather than a caveat.

A clamped registration's declared frontier descends only while the gap it is
clamped at is healing: each chunk lowers it, and when that gap resolves the
handoff moves the registration to the nearest open gap below. **A deferred
gap never resolves**, so the frontier stops at the first deferred gap below
the wall and stays there — permanently, however much history heals further
down. Rows below it are stored and queryable and the gap list says exactly
what is missing, but `indexedFromLedger` does not move again.

So **the gaps immediately below a registration's clamp must be planned as
replay**, or its coverage never advances at all. The case to watch is the
wall-drift gaps: the few tiny ranges recorded as the retention wall moved
during a long walk sit directly under the clamp, are easy to dismiss as
negligible — tens of ledgers — and deferring even one of them freezes the
declared coverage of every contract in the batch. Their cost is a captive
core spin-up each, which is the right price.

Teaching the API to express "covered except these declared holes" would
retire the whole edge. It is a worthwhile follow-up and an
`api-surface-change`; it is out of scope here and nothing in this spec
depends on it.

## 3. The hint is an input, never a dependency

**Sierpe does not fetch a hint from anywhere.** The operator supplies a replay
plan — a set of intervals — through the admin API. The instance has no oracle
client, no credentials, no new dependency (D6), and no way to spend money.

For the pilot the plan came from a BigQuery query over the public
`crypto-stellar` Hubble dataset, but any source works: another indexer, an
analytics warehouse, a previous Sierpe instance, a hand-written list.

### What the operator's plan must contain

The plan is an over-approximation of where a registered contract can have
produced a row. Two properties matter, and one of them is subtle:

- **Events are a sound hint for events, state and movements — validated.**
  On the 500k-ledger window that was already replayed linearly (ground truth:
  13 event ledgers, 12 state-change ledgers, 2 movement ledgers), every ledger
  with a state change or a movement also had an event: **zero ledgers outside
  the event set**, and the hint reproduced the replayed ledger set exactly,
  with zero false negatives. That is empirical for this workload, not a
  theorem.
- **A contract's own deployment is a real hole, and it was measured.** State
  changes derive from `LedgerEntryTypeContractData` changes only — TTL entries
  are never extracted, so a bare `extendFootprintTTL` produces nothing and
  needs no hint. But the creation of a contract's instance at deploy time, and
  the restore of an archived entry, *are* contract-data changes and may carry
  no event. The pilot's 1,285 registered contracts were deployed across 1,270
  distinct ledgers, and **108 of those contracts — 8.4% — were deployed in a
  ledger holding no event at all**, which puts 108 deployment ledgers outside
  an events-only hint. Padding would not have rescued them, since a deployment
  can sit months away from the nearest cluster. The common pattern does carry an event — the pilot
  contract deployed and initialised in the same ledger, 59,146,455 — but the
  tail is large enough to matter. **The plan must include each registered
  contract's deployment ledger explicitly.**

  Deployments above the RPC retention wall need no plan entry: that history is
  inside the window the ordinary backfill walks.

### Scoping the hint to the registered set

A plan is only valid for the contract set it was computed from. This is not a
footnote: during the pilot's validation an unscoped comparison produced 4,022
"active" ledgers against 13 replayed, because the hint covered 1,285
contracts and the ground truth covered 3.

The consequence is a hard rule: **registering a contract invalidates every
deferred gap inside its target range.** Registration must flip those gaps back
to `replay`, because the deferral was justified by a hint that did not know
about this contract. An operator who wants the sparse behaviour for the new
contract submits a new plan. The safe direction is automatic; the fast one is
explicit.

## 4. Mechanics

### Schema

```sql
ALTER TABLE gaps ADD COLUMN heal_mode text NOT NULL DEFAULT 'replay';
-- 'replay'   : the healer owns this range and will replay it
-- 'deferred' : deliberately not replayed; open, declared, never claimed
```

`deferred` says what was decided, not what is true: it is not a claim that the
range is empty.

`ListOpenGaps` returns only `heal_mode = 'replay'`, so the healer never walks a
desert. Everything else that reads gaps — `/status`, the open-gap count, the
clamp handoff in `CommitHealChunk` — keeps seeing all open gaps, which is what
makes the declared coverage stay conservative for free.

### Admin operation

```
POST /v1/admin/gaps/plan
{ "network": "mainnet",
  "replay": [[59146455, 59152703], [59161088, 59161215], ...] }
```

Reconciles a set, does not apply an event (rule 11). Given the intervals to
replay, it rewrites the affected open gaps so that the union of the resulting
rows is exactly the original range, `replay` where the plan asks and
`deferred` everywhere else. Submitting the same plan twice changes nothing;
submitting a wider plan converts deferred ranges back to replay.

It must not go through `RecordGap`, whose floor-trimming against open gaps
exists for a different purpose and would fight the split.

Ranges already healed are untouched: the operation only ever partitions what
is still open. That is safe because a healed stretch stops being a promise
only to the contracts it was healed for — and the moment a later batch
clamps over it, `RecordGap` rewinds the gap to owe the stretch again, so the
next plan partitions it like any other missing range.

Apply a plan with the healer idle. A split replaces the gap row, and a
worker holding that row mid-chunk fails its commit with `gap vanished or
resolved mid-heal`, losing the chunk's replay. Nothing is corrupted — the
watermark only moves on commit — but on a deep gap that is hours of work
thrown away.

### Checkpoint alignment and padding

Bounded captive replay catches up from the published checkpoint at or below
its start, so a cluster whose start is mid-checkpoint pays up to 63 re-applied
ledgers. Cluster starts snap down to the enclosing checkpoint boundary and
ends snap up to a checkpoint close (`checkpointFrequency = 64`), which also
makes the resulting gap ids deterministic.

Alignment is the endpoint's job, not the operator's: a plan carries the
intervals where activity is believed to be, and the server snaps them. Each
interval is padded by a configurable margin (a few hundred ledgers by default)
before alignment; an operator whose plan is already padded sets the margin to
zero rather than paying for it twice. Padding is cheap — a few hundred ledgers
cost seconds against an hour of spin-up — and absorbs edge effects in the
hint. It does **not** rescue an activity that sits far from any cluster; that
is what the deployment-ledger rule in §3 is for.

### Workers

`HEAL_WORKERS` (default 1) lets an operator replay several gaps
concurrently. Each worker takes a distinct gap; two workers never share one.
The equivalence gate (P5) stays a once-per-process affair and runs before any
worker starts.

**RAM is the binding constraint, and the sum is over the whole machine.**
Each worker is a captive core; the pilot measured one at **7.6 GB resident
at rest and 9.8 GB at peak**, and a second chunk at **8.95 GB**. The budget is total host memory minus
everything else the machine runs — not the free memory of the moment, which
looks generous right up until the peak arrives. The pilot's 15.4 GB host
with ~1.6 GB of other services therefore supports **one** worker: two would
want 15–20 GB of core alone, and being killed mid-chunk costs the whole
chunk's replay, which can be a day of work.

That footprint does not shrink with smaller clusters, which is the tempting
way to fit a second worker. A catchup's memory is dominated by the bucket
state of its anchor checkpoint — the size of the network, not the length of
the range being replayed.

The pilot measured both ends of that claim on real heal chunks. A
**500,000-ledger** chunk peaked at 9.8 GB. A **nine-ledger** chunk — one of
the wall-drift gaps — peaked at **8.95 GB**. Same band, for ranges differing
by a factor of fifty-five thousand. Range width is not a lever on memory,
and a host that cannot afford two workers for a wide chunk cannot afford
them for a narrow one either. Deep checkpoints are somewhat cheaper because
the network was smaller then, but that is a property of how far back a
cluster sits, not of how wide it is.

The pilot's planned heal, measured rather than modelled: 62 replay gaps over
643,397 ledgers, 62 spin-ups at ~13 min plus 36.6 h of replay, about **50 h
on the one worker its host can afford** — against roughly two weeks of
linear replay for the same history. Two workers would halve it; on this
hardware that is a memory upgrade, not a configuration change.

### Observability

`/status` and `/metrics` must separate the two kinds of open gap, or an
operator reads "58 open gaps" and concludes the heal is broken:

- `sierpe_deferred_gaps` and `sierpe_deferred_ledgers` alongside the existing
  open-gap counters;
- `/status` reports deferred gaps and their ledger total as their own fields.

The subtraction has to be served, not left to the reader. **Open gaps stop
reaching zero the moment a plan defers anything**, and the obvious
completion check — `open_gaps == 0` — then waits for a condition that can
never occur again, without erroring. That is a silent failure in every
watcher, alert and dashboard written before the plan existed. `/status`
therefore carries `gaps_pending_heal` (open minus deferred) as its own
field, and the metric docs name `sierpe_open_gaps - sierpe_deferred_gaps`
as the completion signal. An instance that never plans anything is
unaffected: its deferred count is zero and the two numbers agree.

## 5. What this does not change

- The equivalence gate. Replayed data is still proven byte-equivalent to the
  RPC before any heal commits (P5).
- Atomicity. A chunk still commits its rows and its watermark in one
  transaction (rule 1).
- Whole-ledger ingestion. Sparse heal selects *which ledgers to replay*, never
  which parts of a ledger to fetch; every replayed ledger is still filtered
  locally against the registry (D1, rule 2). Registration has always driven
  the range walked — `target_from` does exactly that for backfill — and this
  extends that range from one interval to a set of them.
- Gap honesty. Every ledger of a recorded gap remains inside a recorded gap.
