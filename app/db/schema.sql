-- Idempotent transaction service schema (SQLite)
-- All financial amounts are integer minor units (e.g. paise).

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   TEXT    NOT NULL PRIMARY KEY,                -- business transaction ID
    request_id       TEXT    NOT NULL UNIQUE,                     -- incoming request key (idempotency key)
    account_id       TEXT    NOT NULL,
    transaction_type TEXT    NOT NULL CHECK (transaction_type IN ('CREDIT', 'DEBIT')),
    amount_minor     INTEGER NOT NULL CHECK (amount_minor > 0),
    currency         TEXT    NOT NULL CHECK (length(currency) = 3),
    sequence_number  INTEGER NOT NULL CHECK (sequence_number > 0),
    status           TEXT    NOT NULL DEFAULT 'RECEIVED'
                         CHECK (status IN ('RECEIVED', 'PROCESSING', 'PENDING_ORDER',
                                           'RETRY_PENDING', 'PROCESSED', 'FAILED')),
    attempt_count    INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error       TEXT,
    next_retry_at    TEXT,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_claim
    ON transactions (status, next_retry_at, created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_account_seq
    ON transactions (account_id, sequence_number);

CREATE TABLE IF NOT EXISTS accounts (
    account_id    TEXT    NOT NULL PRIMARY KEY,
    balance_minor INTEGER NOT NULL DEFAULT 0 CHECK (balance_minor >= 0),
    last_sequence INTEGER NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    ledger_id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT    NOT NULL UNIQUE,   -- guarantees one financial effect per business transaction
    account_id     TEXT    NOT NULL,
    amount_minor   INTEGER NOT NULL,          -- signed balance change (+credit / -debit)
    created_at     TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger_entries (account_id);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key TEXT    NOT NULL PRIMARY KEY,
    payload_hash    TEXT    NOT NULL,          -- detects reuse of a key with a different payload
    transaction_id  TEXT    NOT NULL,
    result_status   TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);
