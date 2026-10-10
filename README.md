# Idempotent Transaction Service

A production-aware transaction processing engine: **durable, idempotent,
order-aware and crash-safe**. Built with FastAPI + SQLite, a background
worker, bounded retries, and a 40-case test suite that proves the behavior
under duplication, reordering, and mid-processing failure.

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688)
![SQLite](https://img.shields.io/badge/SQLite-WAL-003b57)
![Tests](https://img.shields.io/badge/tests-40%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## Table of contents

- [Why this exists](#why-this-exists)
- [Guarantees at a glance](#guarantees-at-a-glance)
- [Tech stack](#tech-stack)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Data model](#data-model)
- [Processing rules](#processing-rules)
- [Status lifecycle](#status-lifecycle)
- [Getting started](#getting-started)
- [Running the service](#running-the-service)
- [Running with Docker](#running-with-docker)
- [Configuration reference](#configuration-reference)
- [API reference](#api-reference)
- [Worked examples](#worked-examples)
- [End-to-end demo](#end-to-end-demo)
- [Testing](#testing)
- [Fault injection](#fault-injection)
- [Design choices and trade-offs](#design-choices-and-trade-offs)
- [Known limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)
- [AI assistance disclosure](#ai-assistance-disclosure)

---

## Why this exists

Real transaction inputs are messy. They get **delivered more than once**,
they **arrive out of order**, and the system sometimes **fails halfway
through applying one**. A naive service double-books payments, applies a
withdrawal before its deposit, or leaves an account balance changed without a
matching ledger entry.

This service treats those three realities as the core problem. It accepts
transactions over HTTP (single or batch), persists them durably, and processes
them asynchronously with correctness guarantees that hold even under failure —
and it proves every guarantee with tests.

## Guarantees at a glance

| Guarantee | How it is enforced | Where it is tested |
|-----------|--------------------|--------------------|
| **No double financial effect** | Unique `transaction_id` on ledger + business-uniqueness check at ingest and apply | `test_idempotency.py`, `test_consistency.py` |
| **Safe request replay** | `idempotency_keys.payload_hash` distinguishes replay from key misuse | `test_idempotency.py` |
| **Conflict on key reuse** | Same key, different payload → `409` and no new job | `test_idempotency.py` |
| **Strict per-account ordering** | `accounts.last_sequence` gate; early arrivals wait as `PENDING_ORDER` | `test_ordering.py` |
| **Deterministic ordering recovery** | On apply, the next successor is re-queued automatically | `test_ordering.py` |
| **Atomic balance + ledger** | One `BEGIN IMMEDIATE` transaction for ledger insert + balance + sequence + status | `test_consistency.py` |
| **Bounded transient retries** | Attempt counter + exponential backoff, max `TXN_MAX_ATTEMPTS` | `test_retry.py` |
| **Fast-fail business errors** | Insufficient funds → `FAILED` in one attempt, never retried | `test_retry.py` |
| **Crash-safe durability** | Queue state lives in the database; restart resumes pending work | `test_consistency.py` |
| **Safe concurrent claiming** | Guarded status `UPDATE` + write lock; exactly one worker wins | `test_consistency.py` |

## Tech stack

- **Python 3.11+** (developed and tested on 3.13)
- **FastAPI** — HTTP API and request validation via Pydantic v2
- **SQLite (WAL mode)** — embedded durable job queue + state store
- **Uvicorn** — ASGI server
- **Pytest + httpx** — test suite and TestClient
- **No external services required** — clone, install, run.

## Architecture

```
REST API / JSON batch
        │  1. validate schema
        │  2. request idempotency (replay / conflict)
        │  3. business uniqueness (transaction_id)
        ▼  4. persist as RECEIVED  ──►  202 Accepted
SQLite durable job queue          transactions · accounts
   (job table + state)            ledger_entries · idempotency_keys
        │  5. claim (BEGIN IMMEDIATE + guarded status UPDATE)
        ▼
Background worker ──► ordering gate ──► business idempotency re-check
        │                │ apply atomically (ledger + balance + sequence + status)
        ▼                │ transient fault  → RETRY_PENDING (bounded backoff)
   ledger entries        │ business fault   → FAILED (fast, no retry)
        ▲                └ successor recovery → re-queue PENDING_ORDER
   metrics/summary, statuses, structured JSON logs
```

**Layer responsibilities**

| Layer | Module | Responsibility |
|-------|--------|----------------|
| API | `app/api/routes.py` | Parse input, map results to status codes. No business rules. |
| Domain | `app/models/transaction.py` | Request schemas, field constraints, status enum. |
| Services | `app/services/*` | All business rules: idempotency, ordering, retry policy, orchestration. |
| Worker | `app/worker/processor.py` | Claim and run the processing pipeline asynchronously. |
| Persistence | `app/db/*` | Connections, explicit transactions, row read/write. Decides nothing. |

Full details in [docs/architecture.md](docs/architecture.md).

## Repository layout

```
idempotent-transaction-service/
├── app/
│   ├── main.py                     FastAPI app + lifecycle (embedded worker)
│   ├── config.py                   Env-driven settings
│   ├── api/routes.py               7 endpoints
│   ├── models/transaction.py       Schemas, constraints, Status enum
│   ├── services/
│   │   ├── transaction_service.py  Ingestion + processing orchestration
│   │   ├── idempotency_service.py  Replay vs conflict
│   │   ├── ordering_service.py     Per-account sequence gate
│   │   └── retry_service.py        Bounded retry policy
│   ├── worker/processor.py         Durable poll-and-claim loop
│   └── db/
│       ├── database.py             Connections + BEGIN IMMEDIATE tx
│       ├── repository.py           Pure persistence
│       └── schema.sql              4 tables + invariants
├── tests/                          40 pytest cases
├── sample_data/transactions.json   Demo corpus
├── scripts/run_batch.py            End-to-end demo
├── docs/                           architecture · design-decisions · production-readiness
├── Dockerfile · requirements.txt · .env.example
└── README.md
```

## Data model

Four tables, defined in `app/db/schema.sql`. All amounts are integer **minor
units** (e.g. paise), never floats.

### `transactions` — the job and its lifecycle

| Column | Purpose |
|--------|---------|
| `transaction_id` (PK) | Unique business transaction ID |
| `request_id` (UNIQUE) | Incoming request key — doubles as the idempotency key |
| `account_id` | Account being modified |
| `transaction_type` | `CREDIT` or `DEBIT` (CHECK constraint) |
| `amount_minor` | Positive integer minor units (`CHECK > 0`) |
| `currency` | ISO code, validated against supported set |
| `sequence_number` | Account-level ordering (`CHECK > 0`) |
| `status` | Lifecycle state (`CHECK` over the status enum) |
| `attempt_count` | Processing attempts so far |
| `last_error` | Sanitized machine-readable failure code |
| `next_retry_at` | Backoff schedule for `RETRY_PENDING` |
| `created_at`, `updated_at` | ISO-8601 UTC timestamps (sortable) |

Indexes: `(status, next_retry_at, created_at)` for claim scanning,
`(account_id, sequence_number)` for ordering checks.

### `accounts` — balance and ordering state

| Column | Purpose |
|--------|---------|
| `account_id` (PK) | Account identifier |
| `balance_minor` | Current balance (`CHECK >= 0` — negative balance is impossible at the schema level) |
| `last_sequence` | Last successfully applied account sequence |

### `ledger_entries` — immutable financial effects

| Column | Purpose |
|--------|---------|
| `ledger_id` (PK) | Entry identifier |
| `transaction_id` (**UNIQUE**) | One financial effect per business transaction, enforced by the schema |
| `account_id` | Account affected |
| `amount_minor` | Signed balance change (+credit / -debit) |
| `created_at` | Application timestamp |

### `idempotency_keys` — request replay and conflict detection

| Column | Purpose |
|--------|---------|
| `idempotency_key` (PK) | The request key |
| `payload_hash` | SHA-256 of the canonical business fields — detects key reuse with a different payload |
| `transaction_id` | Associated business transaction |
| `result_status` | Recorded outcome, refreshed as the job progresses |

## Processing rules

Implemented in this order in `app/services/transaction_service.py`:

1. **Validate input** — schema constraints (`app/models/transaction.py`):
   positive amount, supported currency, valid type, sequence ≥ 1, well-formed
   IDs. In the batch path, an invalid record is isolated; the rest continue.
2. **Check request idempotency** — same key + same `payload_hash` → replay the
   recorded outcome (`200`). Same key + different payload → `409 Conflict`, no
   new job.
3. **Check business uniqueness** — same `transaction_id` under a different
   request → `duplicate`, never a second effect. A duplicate delivery never
   overwrites the original transaction's status.
4. **Check ordering** — only the next expected sequence per account applies.
   An early arrival is persisted as `PENDING_ORDER`.
5. **Apply atomically** — in one `BEGIN IMMEDIATE` transaction: insert the
   ledger entry, update balance and sequence (guarded by
   `WHERE last_sequence = n-1`), mark `PROCESSED`. Any failure rolls back all
   of it.
6. **Retry transient errors** — increment `attempt_count`, schedule
   `RETRY_PENDING` with exponential backoff. Past the limit → `FAILED`, fully
   observable and manually recoverable.
7. **Report results** — status counts and ledger totals via `/metrics/summary`;
   sanitized `last_error` codes; account IDs masked in logs.

## Status lifecycle

```
RECEIVED ──► PROCESSING ──► PROCESSED
                │
                ├──► PENDING_ORDER ──► (predecessor applies) ──► RECEIVED
                ├──► RETRY_PENDING ──► (backoff due) ─────────► PROCESSING
                └──► FAILED ─────────► (manual POST /retry) ──► RECEIVED
```

`DUPLICATE` is an **ingestion outcome** reported about a request; it is never
a state on the original transaction. Terminal `FAILED` is recoverable by an
operator, not automatically.

## Getting started

```bash
git clone https://github.com/majjiharisivaramprakash-eng/idempotent-transaction-service
cd idempotent-transaction-service

# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate         # Windows PowerShell
# source .venv/bin/activate    # macOS / Linux

# Install dependencies
pip install -r requirements.txt

# Optional: copy the example env (every setting has a sane default)
copy .env.example .env         # Windows
# cp .env.example .env         # macOS / Linux
```

Verify the install by running the tests (see [Testing](#testing)) or starting
the server (see [Running the service](#running-the-service)).

## Running the service

### Default: one process, API + embedded worker

```bash
uvicorn app.main:app --reload
```

- Interactive API docs: http://127.0.0.1:8000/docs
- Health check: http://127.0.0.1:8000/health

The background worker runs inside the API process (asyncio task), so a single
command runs the whole system. Ideal for review and local testing.

### Production-style: separate API and worker processes

```bash
# Terminal 1 — API only (no worker)
set TXN_WORKER_MODE=off          # Windows; export TXN_WORKER_MODE=off on Unix
uvicorn app.main:app

# Terminal 2 — standalone durable worker
set TXN_WORKER_MODE=external
python -m app.worker.processor
```

Because the queue lives in the database, the worker can be restarted, scaled,
or run on another host without losing pending jobs.

## Running with Docker

```bash
docker build -t idempotent-transaction-service .

docker run -p 8000:8000 \
  -v txn_data:/data \
  idempotent-transaction-service
```

The image starts the API with the embedded worker on port 8000 and stores the
SQLite database in `/data` (mounted volume). For a split topology, set
`TXN_WORKER_MODE=off` on the API container and run
`python -m app.worker.processor` in a worker container sharing the volume.

## Configuration reference

All settings are environment variables with sensible defaults; see
`.env.example`. Relative DB paths resolve from the repo root.

| Variable | Default | Meaning |
|----------|---------|---------|
| `TXN_DB_PATH` | `txn.db` | SQLite file location |
| `TXN_WORKER_MODE` | `embedded` | `embedded` \| `external` \| `off` |
| `TXN_POLL_INTERVAL_SECONDS` | `1.0` | Worker idle poll interval |
| `TXN_MAX_ATTEMPTS` | `3` | Processing attempts before `FAILED` |
| `TXN_BACKOFF_BASE_SECONDS` | `1.0` | Retry backoff base (`base * 2^(attempt-1)`) |
| `TXN_FAULT_TRANSIENT_AMOUNT` | `1111` | Sentinel amount that fails once, then succeeds |
| `TXN_FAULT_PERMANENT_AMOUNT` | `2222` | Sentinel amount that fails every attempt |

## API reference

| Method | Endpoint | Purpose | Success codes |
|--------|----------|---------|---------------|
| `GET` | `/health` | Liveness + DB check | `200` |
| `POST` | `/transactions` | Submit a single transaction | `202` accepted, `200` replayed/duplicate, `409` conflict, `422` invalid |
| `POST` | `/transactions/batch` | Submit many; per-record isolation | `200` with counts |
| `GET` | `/transactions/{transaction_id}` | Status, attempts, error detail | `200`, `404` |
| `POST` | `/transactions/{transaction_id}/retry` | Re-queue a terminal `FAILED` job | `202`, `409` not retryable, `404` |
| `GET` | `/accounts/{account_id}` | Balance and last sequence | `200`, `404` |
| `GET` | `/metrics/summary` | Counts by status + ledger totals | `200` |

**Request schema** (`POST /transactions`):

```json
{
  "request_id": "req-1001",       // idempotency key, [A-Za-z0-9_-]{6,64}
  "transaction_id": "txn-1001",   // business ID,   [A-Za-z0-9_-]{6,64}
  "account_id": "ACC-1001",       // [A-Za-z0-9_-]{3,32}
  "transaction_type": "CREDIT",   // CREDIT | DEBIT
  "amount_minor": 500000,         // integer > 0, minor units (paise)
  "currency": "INR",              // supported: INR
  "sequence_number": 1            // integer >= 1, per-account order
}
```

## Worked examples

### 1. Accept a transaction (queued, not yet processed)

```bash
curl -X POST http://127.0.0.1:8000/transactions \
  -H "Content-Type: application/json" \
  -d '{"request_id":"req-1001","transaction_id":"txn-1001","account_id":"ACC-1001",
       "transaction_type":"CREDIT","amount_minor":500000,"currency":"INR","sequence_number":1}'
```

```json
// 202 Accepted
{
  "accepted": true,
  "request_id": "req-1001",
  "transaction_id": "txn-1001",
  "status": "RECEIVED",
  "message": "Transaction persisted and queued for processing."
}
```

### 2. Replay vs conflict

Submitting the **identical** body again returns `200` with `"replayed": true`
and the live status — no second job, no second effect. Reusing `req-1001` with
a **different** amount returns:

```json
// 409 Conflict
{
  "accepted": false,
  "request_id": "req-1001",
  "error": "IDEMPOTENCY_KEY_REUSED",
  "message": "This request key was already used with a different payload. Submit it unchanged or use a new request key."
}
```

### 3. Status detail

```bash
curl http://127.0.0.1:8000/transactions/txn-1001
```

```json
{
  "transaction_id": "txn-1001",
  "request_id": "req-1001",
  "account_id": "ACC-1001",
  "transaction_type": "CREDIT",
  "amount_minor": 500000,
  "currency": "INR",
  "sequence_number": 1,
  "status": "PROCESSED",
  "attempt_count": 0,
  "last_error": null,
  "next_retry_at": null,
  "created_at": "2026-10-10T09:00:00.000000+00:00",
  "updated_at": "2026-10-10T09:00:01.000000+00:00"
}
```

### 4. Recover a terminal failure

```bash
curl -X POST http://127.0.0.1:8000/transactions/txn-2003/retry
# 202 -> status back to RECEIVED with a fresh attempt budget
```

### 5. Metrics

```bash
curl http://127.0.0.1:8000/metrics/summary
```

```json
{
  "counts_by_status": { "PROCESSED": 5, "FAILED": 2, "PENDING_ORDER": 2 },
  "total_transactions": 9,
  "processed_after_retry": 1,
  "retry_outcomes": { "retry_pending": 0, "failed": 2 },
  "ledger": {
    "entries": 5,
    "total_credit_minor": 1051111,
    "total_debit_minor": 100000,
    "net_minor": 951111
  }
}
```

## End-to-end demo

With the server running:

```bash
python scripts/run_batch.py
```

It posts `sample_data/transactions.json` — a deliberate mix of duplicates,
conflicts, out-of-order sequences, invalid records and fault-triggering
amounts — waits for the worker to settle the queue, and prints the final
outcome table, balances and metrics.

### Actual output from a clean run

```
Submitting 14 sample records (duplicates, conflicts, out-of-order, invalid and fault-triggering)...

Ingestion outcomes: {"accepted": 9, "replayed": 1, "duplicate": 1, "conflict": 1, "invalid": 2}

transaction_id account     seq type        amount status         attempts  last_error
-------------------------------------------------------------------------------------
txn-1001       ACC-1001      1 CREDIT      500000 PROCESSED             0
txn-1002       ACC-1001      2 CREDIT      250000 PROCESSED             0
txn-1003       ACC-1001      3 DEBIT       100000 PROCESSED             0
txn-2001       ACC-2002      1 CREDIT      300000 PROCESSED             0
txn-2002       ACC-2002      2 CREDIT        1111 PROCESSED             1
txn-2003       ACC-2002      3 CREDIT        2222 FAILED                3  RETRY_EXHAUSTED after 3 attempts (TRANSIENT_FAILURE)
txn-2004       ACC-2002      4 CREDIT       77777 PENDING_ORDER         0
txn-3001       ACC-3003      1 DEBIT        50000 FAILED                1  INSUFFICIENT_FUNDS
txn-4005       ACC-4004      5 CREDIT      100000 PENDING_ORDER         0

Account ACC-1001: balance=650000 paise last_sequence=3
Account ACC-2002: balance=301111 paise last_sequence=2

Ledger: 5 entries, total_credit=1051111, total_debit=100000, net=951111
```

What this proves, in one table:

- `txn-1002` (sequence 2) was submitted **before** `txn-1001` (sequence 1),
  yet both applied in the correct order — ordering + successor recovery.
- `txn-2002` hit a transient failure and **still produced exactly one** ledger
  effect after retry.
- The `replayed`, `duplicate` and `conflict` submissions produced **no extra
  rows or financial effects**.
- `txn-3001` (overdraft) failed fast with no retry churn; `txn-2003` exhausted
  its bounded retries; the blocked successors stayed observable.

## Testing

```bash
pytest              # full suite
pytest -v           # per-case names
pytest tests/test_consistency.py -v   # a single area
```

40 tests cover the must-pass matrix:

| File | Covers |
|------|--------|
| `test_idempotency.py` | duplicate request, key reuse conflict, business duplicate, replay returns live status |
| `test_ordering.py` | out-of-order deferral, in-order recovery, permanent gap, sequence conflict |
| `test_retry.py` | transient retry then single effect, exhaustion, business fast-fail, manual recovery |
| `test_validation.py` | invalid fields rejected, batch isolation, per-field error detail |
| `test_consistency.py` | mid-update rollback, two-worker claim contention, ledger uniqueness, redelivery-after-commit, restart recovery |
| `test_api.py` | endpoint contracts, 202 vs 200 vs 409 semantics, metrics |

Each test runs against a fresh file-based SQLite database with the embedded
worker disabled, so processing is driven explicitly and results are fully
deterministic.

## Fault injection

Deterministic sentinels make failures reproducible without chaos infrastructure:

| Amount (minor units) | Behavior |
|----------------------|----------|
| `1111` | Fails the first attempt, succeeds on retry |
| `2222` | Fails every attempt → `RETRY_EXHAUSTED` |
| DEBIT > balance | `INSUFFICIENT_FUNDS`, fast-fail, never retried |

Sentinels are configurable via `.env`. Tests additionally use an in-process
chaos hook (`TransactionService._chaos_hook`) that raises **mid-transaction**,
right after the ledger insert, to prove the entire financial effect (ledger +
balance + sequence + status) rolls back together — never partially.

## Design choices and trade-offs

The reasoning behind each non-obvious decision is documented in
[docs/design-decisions.md](docs/design-decisions.md), including:

- Why `request_id` doubles as the idempotency key and how `payload_hash`
  separates replay from conflict.
- Why ordering is strict and event-driven rather than timer-based.
- Why transient faults retry but business faults fast-fail.
- How a single `BEGIN IMMEDIATE` transaction guarantees atomic balance+ledger.
- What exactly prevents two workers from double-applying.
- The migration path to **PostgreSQL + Kafka** (row locks, `SKIP LOCKED`,
  transactional outbox, DLQ).

## Known limitations

Deliberate, documented trade-offs for this scope:

- **SQLite** uses a single-writer lock; fine for correctness and moderate
  throughput, not for high-concurrency horizontal scaling (see the Postgres
  migration in the design doc).
- **Async ingestion** — `POST` acknowledges persistence (`202`), not
  completion; clients poll status or read metrics. No webhook/callback yet.
- **Head-of-line blocking** — a terminally failed transaction blocks later
  sequences on that account by design; operators resolve it via the retry
  endpoint. A production system would add a void/skip-with-compensation
  workflow.
- **No delivery leases** — a worker that crashes after claiming leaves the row
  stuck in `PROCESSING` (safe: no double effect, but delayed). Production would
  add `claimed_by` + lease expiry with a reaper.
- **Accounts auto-create at zero balance** on first reference; no separate
  registration step.
- **Single currency** for balances (multi-currency is a documented production
  extension).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Address already in use` on `:8000` | A previous server is still running | Stop it (Ctrl+C) or use `--port 8001` |
| `ModuleNotFoundError: app` | Not in the repo root or venv inactive | `cd idempotent-transaction-service` and activate the venv |
| Transactions stuck in `RECEIVED` | Worker disabled (`TXN_WORKER_MODE=off`) and no external worker running | Start a worker or use default `embedded` mode |
| Everything ends `FAILED`/`PENDING_ORDER` | You submitted a fault-sentinel amount or a gap sequence | Expected; check `last_error` and the retry endpoint |
| Tests fail with DB lock errors | Leftover `*.db` in the repo | Remove local `*.db*` files; tests use their own temp DB |

## AI assistance disclosure

This project was built with AI assistance (spec-to-code generation and test
drafting), then validated manually: every guarantee maps to a named test, the
failure paths were reviewed line by line, and the end-to-end demo output was
verified against hand-computed expected balances and ledger counts. See
[docs/design-decisions.md](docs/design-decisions.md), question 7, for the full
breakdown of what was AI-assisted and how it was checked.

---

**Definition of done** — core requirements implemented; duplicates never create
a second effect; ordering and retries documented and tested; all critical state
changes atomic; tests pass from a clean environment; sample output and
operational summaries available; a reviewer can run it from this README; and
the repository contains no secrets, local DB files, or generated artifacts.
