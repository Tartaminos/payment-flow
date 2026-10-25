ALTER TABLE payment_operations
    ADD COLUMN lease_owner UUID,
    ADD COLUMN lease_until TIMESTAMPTZ;

CREATE TABLE reconciliation_jobs (
    id UUID NOT NULL,
    operation_id UUID NOT NULL,
    unknown_since TIMESTAMPTZ NOT NULL,
    next_check_at TIMESTAMPTZ NOT NULL,
    lease_owner UUID,
    lease_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(100),
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_reconciliation_jobs PRIMARY KEY (id),
    CONSTRAINT uk_reconciliation_jobs_operation UNIQUE (operation_id),
    CONSTRAINT fk_reconciliation_jobs_operation FOREIGN KEY (operation_id) REFERENCES payment_operations (id),
    CONSTRAINT ck_reconciliation_jobs_attempts_nonnegative CHECK (attempts >= 0)
);

CREATE INDEX ix_reconciliation_jobs_due ON reconciliation_jobs (resolved_at, lease_until, next_check_at);
