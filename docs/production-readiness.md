# Production Readiness

This assessment implementation makes correctness guarantees explicit with
SQLite. This document states precisely what it would take to operate the same
engine at production scale and reliability.

## 1. Storage: SQLite → PostgreSQL

| Concern | SQLite today | PostgreSQL production |
|---|---|---|
| Job claiming | `BEGIN IMMEDIATE` + guarded UPDATE (serialized writers) | `SELECT ... FOR UPDATE SKIP LOCKED`, horizontal workers |
| Atomic effects | Single DB transaction | Same, plus row-level locks on `accounts` |
| Sequence guard | `WHERE last_sequence = n-1` + rowcount | Identical pattern, port `RETRY_PENDING` to `next_retry_at timestamptz` |
| Durability | WAL file on host volume | Managed backups, PITR, replica promotion |
| Constraint invariants | UNIQUE + CHECK constraints | Identical constraints; add `NOT VALID` migration strategy |

Migration is schema-compatible by design: every constraint used here has a
direct Postgres equivalent.

## 2. Input: REST → event broker (Kafka)

- Partition by `account_id` to preserve per-account arrival order; the
  `PENDING_ORDER` mechanism remains as the safety net for retries and
  rebalances.
- Consumer commits offsets only after the ingestion DB transaction commits
  (transactional outbox), closing the "crash between accept and persist" gap.
- `FAILED` records are published to a dead-letter topic with the sanitized
  error code plus a correlation ID; the table remains the system of record.

## 3. Delivery semantics gaps to close

- **Leased claiming**: today a crashed worker leaves a `PROCESSING` row stuck
  (safe: no duplicate effect, but delayed). Production adds
  `claimed_by` / `lease_expires_at` and a reaper that returns expired leases
  to the queue.
- **Crash between commit and ack**: already safe — redelivery hits the
  ledger-existence check and re-marks `PROCESSED` with no new effect (tested).
- **Duplicate key expiry policy**: `idempotency_keys` grows forever;
  production defines a retention window tied to the maximum realistic retry
  delay, with archived keys kept auditable.

## 4. Observability

- Currently: structured JSON logs with masked account IDs, per-status metrics
  via `/metrics/summary`, sanitized machine-readable `last_error` codes.
- Production adds: OpenTelemetry traces per job (ingest→apply), counters for
  `replay/conflict/duplicate` at ingestion, queue-depth and age histograms
  (`now - created_at` per status), alerting rules:
  - any `RETRY_EXHAUSTED` → paging after N per window
  - `PENDING_ORDER` age > SLO → gap investigation
  - claim-rate vs completion-rate divergence → stuck-worker alarm
- `/health` becomes liveness + readiness (DB writable, worker heartbeating).

## 5. Security and compliance

- No secrets exist in the repo; production injects credentials via a secrets
  manager, TLS on every hop.
- Authentication/authorization per endpoint; account-level tenancy checks
  (`GET /accounts/{id}` must not be globally readable).
- Ledger is append-only: enforce with DB permissions (no UPDATE/DELETE for
  the app role) plus periodic hash-chain or external notary verification.
- Data retention & right-to-be-forgotten policy for `account_id` references;
  current logs already minimize account data (prefix-masked).
- Reconciliation job: `SUM(ledger signed amounts) == accounts.balance_minor`
  per account, per shard — run continuously, alarm on drift. (This service
  can detect drift with the same query today; production schedules it.)

## 6. Operations

- Horizontal scaling: workers are stateless; SQLite's single-writer lock is
  the only barrier — Postgres removes it entirely.
- Zero-downtime deploys: drain = stop claiming, let leases finish; durable
  queue means restarting mid-batch resumes exactly where it stopped (tested).
- Migration discipline: alembic-style versioned schema, expand-contract.
- Failure drills in CI: kill worker between claim and commit; inject DB
  errors mid-transaction; replay day-old broker partitions against prod
  snapshot.

## 7. Explicitly out of scope here (and why)

- Multi-currency / FX: `currency` is validated and stored, but balances are
  single-currency. A production ledger keys balances by (account, currency).
- Authorization, KYC, sanctions screening: ingestion assumes trusted
  submitters.
- Head-of-line blocking workflow (void/skip with compensating entries):
  the mechanism is observable and operator-recoverable here; a full
  exception-management UI is a production concern.
