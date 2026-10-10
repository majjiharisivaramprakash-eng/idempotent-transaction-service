# Architecture

## Goal

Process financial transactions correctly under three unavoidable realities of
distributed systems: messages are delivered more than once, messages arrive
out of order, and processing fails partway through.

## System view

```
┌──────────────────────────── Input adapters ────────────────────────────┐
│  REST API (single)          REST API (batch)        [replaceable:     │
│  POST /transactions         POST /transactions/batch Kafka consumer,  │
│                                                    cron file loader]  │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │ 1. validate schema
                               │ 2. request idempotency (replay / conflict)
                               │ 3. business uniqueness (transaction_id)
                               │ 4. persist as RECEIVED  ──►  202 Accepted
┌──────────────────────────────▼─────────────────────────────────────────┐
│                     SQLite — durable queue + state                      │
│  transactions (job lifecycle, attempts, backoff)                         │
│  accounts (balance, last applied sequence)                               │
│  ledger_entries (immutable financial effects, UNIQUE per transaction)    │
│  idempotency_keys (request key → payload hash → recorded outcome)        │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │ 5. claim: BEGIN IMMEDIATE + guarded UPDATE
┌──────────────────────────────▼─────────────────────────────────────────┐
│                            Background worker                            │
│  ordering gate (per-account next sequence)                               │
│  business idempotency re-check (ledger existence)                        │
│  atomic apply: ledger + balance + sequence + status in ONE tx            │
│  retry taxonomy: transient → bounded backoff retry | business → FAILED   │
│  successor recovery: PENDING_ORDER jobs re-queued when unblocked         │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────────────────┐
│                    Operations: statuses, metrics, JSON logs              │
│  GET /transactions/{id} · GET /accounts/{id} · GET /metrics/summary      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Layer responsibilities

| Layer | Module | Responsibility |
|-------|--------|----------------|
| API | `app/api/routes.py` | Parse/map only. No business rules. |
| Domain | `app/models/transaction.py` | Schema constraints, status enum. |
| Services | `app/services/*` | All business rules: idempotency, ordering, retry policy, orchestration. |
| Worker | `app/worker/processor.py` | Claim and run the processing pipeline asynchronously. |
| Persistence | `app/db/*` | Connections, explicit transactions, row read/write. Decides nothing. |

## The durable queue

The `transactions` table *is* the queue. Status is the visibility control:

- `RECEIVED` — claimable
- `RETRY_PENDING` — claimable once `next_retry_at` has passed
- `PENDING_ORDER` — not claimable until a predecessor applies (recovery flips it back to `RECEIVED`)
- `PROCESSING` — owned by exactly one worker
- `PROCESSED` / `FAILED` — terminal

Because queue state lives in the database, a worker crash, a deployment
restart, or a full machine reboot (with the DB file preserved) resumes
exactly where processing stopped — with no in-memory structures to rebuild.

## Correctness mechanisms

1. **Claim exclusivity**: `BEGIN IMMEDIATE` takes the write lock up front;
   the guarded `UPDATE ... WHERE status IN ('RECEIVED','RETRY_PENDING' ...)`
   succeeds for exactly one worker (`rowcount == 1`).
2. **Single financial effect**: `ledger_entries.transaction_id UNIQUE` makes
   a second effect structurally impossible even if every other guard failed.
3. **Atomic effects**: ledger insert + balance + `last_sequence` + status
   commit or roll back as one unit. The sequence guard
   `WHERE last_sequence = seq - 1` makes concurrent double-apply abort.
4. **Replay safety at processing time**: if a committed job is redelivered
   (crash before ack), the ordering layer sees the existing ledger entry and
   re-marks `PROCESSED` without any new effect.
5. **Key reuse detection**: `payload_hash` (SHA-256 of canonical business
   fields) distinguishes *replay* from *different instruction on an old key*.
