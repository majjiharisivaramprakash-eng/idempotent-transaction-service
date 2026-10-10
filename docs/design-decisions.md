# Design Decisions

## 1. Why these idempotency key and business transaction ID rules?

Two independent failure modes need two independent guards:

- **Request-level idempotency** (`request_id` = idempotency key + SHA-256
  `payload_hash`): protects against *transport* retries — the same HTTP call
  delivered twice. Replaying an identical payload must return the recorded
  outcome, not re-execute anything. But a reused key carrying a *different*
  payload is not a retry, it is a client bug or an attack: executing it would
  silently violate the caller's intent, and ignoring it would lose an
  instruction. So it becomes an explicit `409 Conflict`. The hash covers the
  canonical business fields (sorted-key JSON), excluding the key itself.

- **Business-level idempotency** (`transaction_id`, unique across the system):
  protects against *semantic* duplication — the same financial instruction
  arriving under different request keys (e.g. an upstream system re-submits a
  batch file with new request IDs, or two adapters ingest the same source
  event). Detection happens at ingestion *and* again at apply time via the
  ledger: `ledger_entries.transaction_id UNIQUE` means one financial effect
  per business transaction is a schema invariant, not just code behavior.

Why `request_id` doubles as the idempotency key: for this assessment the
caller supplies both IDs, so a separate `Idempotency-Key` header would add a
third concept without adding a guarantee. A production version would accept
an optional header key and treat it as authoritative for transport retries.

A duplicate delivery never overwrites the original transaction's status —
`DUPLICATE` is an outcome reported about a *request*, not a state of the
transaction. This keeps the audit trail honest about the original processing.

## 2. How are out-of-order events processed?

Per-account strict ordering via `accounts.last_sequence`:

- `sequence == last + 1` → apply.
- `sequence > last + 1` → persist as `PENDING_ORDER`. Nothing is lost; the
  job is fully visible via the API with its gap diagnostic.
- `sequence <= last` → either this very transaction already applied (ledger
  entry exists → idempotently re-mark `PROCESSED`) or the slot was consumed
  by a different transaction → terminal `FAILED` with `SEQUENCE_CONFLICT`.

Recovery is deterministic and event-driven, not timer-driven: when a
transaction at sequence `n` commits, the same worker flips any
`PENDING_ORDER` job at `n+1` back to `RECEIVED` in a follow-up transaction,
and the poll loop processes it next. A chain of queued successors unwinds one
step per apply, which keeps each step's ordering re-check honest.

The ordering decision is **re-evaluated inside the apply transaction**
(`BEGIN IMMEDIATE` + `UPDATE accounts ... WHERE last_sequence = n-1`), so two
workers can never both decide "I am next in sequence".

Known consequence: strict ordering head-of-line-blocks a tail sequence when a
predecessor terminally fails. That is deliberate for financial correctness —
auto-skipping would silently reorder the account's history. Operators resolve
it explicitly (fix cause → `POST /transactions/{id}/retry`); a production
system would add a void/skip workflow that writes a compensating audit entry.

## 3. What is the maximum retry policy?

- `TXN_MAX_ATTEMPTS = 3` total processing attempts per job.
- Exponential backoff: `base * 2^(attempt-1)` (`TXN_BACKOFF_BASE_SECONDS`,
  default 1s → 1s, 2s), stored as `next_retry_at` so it survives restarts.
- Only *transient* faults consume the budget: simulated downstream faults,
  database lock/IO errors, and any unexpected exception (fail-safe: unknown
  errors are assumed retryable, but the budget is bounded so they cannot loop
  forever).
- *Deterministic business failures never retry*: insufficient funds on a
  debit is `FAILED` after exactly one attempt. Retrying it cannot change the
  outcome and would mask the real problem.
- Exhaustion → `FAILED` with a sanitized `last_error`
  (`RETRY_EXHAUSTED after 3 attempts (TRANSIENT_FAILURE)`), full row retained
  for investigation. `POST /transactions/{id}/retry` re-queues a terminal job
  with a fresh attempt budget — an explicit operator decision, not an
  automatic behavior.

## 4. How are balance and ledger updates guaranteed atomic?

One explicit `BEGIN IMMEDIATE` database transaction performs, in order:

1. `INSERT INTO ledger_entries ...` (UNIQUE on `transaction_id`)
2. `UPDATE accounts SET balance = balance + signed, last_sequence = n
   WHERE account_id = ? AND last_sequence = n - 1` — a row-count check, not a
   read-then-write
3. `UPDATE transactions SET status = 'PROCESSED'`

Any failure at any step (including a real mid-transaction database failure,
which the test suite injects) rolls all three back together; the job then
goes through the retry path, and the retry either fully applies or fully
rolls back. There is no code path that commits one of the three effects
alone. Additionally `accounts.balance_minor CHECK (>= 0)` means a negative
balance cannot be persisted even if application logic were bypassed.

SQLite caveat: this is per-connection atomicity with database-level write
serialization — correct by construction here. Postgres gets the same pattern
with row locks (see Q6).

## 5. What happens if two workers process the same record?

Three independent layers, each sufficient to prevent a double effect:

1. **Claim exclusivity**: both attempt `UPDATE transactions SET
   status='PROCESSING' WHERE transaction_id=? AND status IN (claimable...)`
   inside `BEGIN IMMEDIATE`. SQLite serializes writers, so exactly one gets
   `rowcount=1`; the other gets `rowcount=0`, skips the job, and moves on.
2. **Sequence guard**: even hypothetically with two concurrent holders, the
   apply step's `WHERE last_sequence = n-1` can succeed for only one.
3. **Schema invariant**: `ledger_entries.transaction_id UNIQUE` — a second
   financial effect raises `IntegrityError` and the whole transaction rolls
   back.

The failure mode of "worker A claims, then crashes" is a *stuck* `PROCESSING`
row, not a duplicated effect. That's a delivery-lease gap, documented in
[production-readiness.md](production-readiness.md): production would add a
reaper that returns expired-lease `PROCESSING` rows to the queue; their
re-processing is safe because of layers 2 and 3.

## 6. What changes with PostgreSQL and Kafka/a message broker?

**SQLite → PostgreSQL** (surgical changes):
- Claiming: `SELECT transaction_id FROM transactions WHERE ... ORDER BY
  created_at FOR UPDATE SKIP LOCKED` (or the same guarded UPDATE, which
  already ports unchanged). No more database-wide write lock; `busy_timeout`
  tuning disappears.
- Isolation: row locks instead of `BEGIN IMMEDIATE`; `NOW()` and `timestamptz`
  replace ISO-string timestamp comparisons.
- `balance_minor CHECK (>= 0)` and all UNIQUE constraints carry over verbatim.
- Partition `transactions`/`ledger_entries` by account or time when volume
  demands; add declarative audit retention.

**REST-only input → Kafka/broker**:
- Add a consumer adapter per topic partition; the *input adapter is already
  isolated* — REST, batch and (now) Kafka all funnel into
  `TransactionService.ingest()`, so ordering/idempotency logic does not move.
- Consume with `account_id` as the partition key → per-account ordering is
  preserved inside one partition, and `PENDING_ORDER` becomes rare (still
  needed: retries inside a partition can still interleave).
- The idempotency check must then also store the broker offset (or message
  fingerprint) alongside `payload_hash` for observability; Kafka-native
  "at-least-once + idempotent sink" makes duplicates *handled*, not *new*.
- Commit offsets only after durable ingestion (`ingest()` returns) —
  an outbox-pattern write in the same DB transaction as the job insert makes
  "consume + persist" atomic; this removes the current REST-era gap where a
  crash between HTTP response and persistence would ask the client to retry.
- `FAILED` jobs get routed to a DLQ topic in addition to remaining in the
  table for manual recovery.

## 7. What was AI-assisted and how was it validated?

AI-assisted parts: project scaffolding, repository/service/worker code
generation, test drafting, and documentation prose. The engineering
decisions — async ingestion semantics, dual-level idempotency model, strict
ordering with head-of-line blocking made *visible* instead of auto-skipped,
the transient-vs-business retry taxonomy, and the three-layer double-apply
defense — were selected and reviewed explicitly.

Validation approach:
- Every guarantee in the README promise list maps to at least one named test
  (`pytest` — 40 cases), including the adversarial ones: mid-transaction
  failures, redelivery-after-commit, two-worker claim contention, key reuse
  with a different payload.
- Failure-path code (rollback, retry bookkeeping, conflict classification)
  was reviewed line-by-line against the design rules, not trusted from
  generation.
- The end-to-end demo (`scripts/run_batch.py`) output was verified against
  hand-computed expected balances, ledger counts and status tallies on a
  clean database.
