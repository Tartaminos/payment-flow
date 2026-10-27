CREATE TABLE reconciliation_escalations (
    id UUID NOT NULL,
    operation_id UUID NOT NULL,
    reconciliation_job_id UUID NOT NULL,
    reason VARCHAR(40) NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'OPEN',
    last_notification_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_reconciliation_escalations PRIMARY KEY (id),
    CONSTRAINT uk_reconciliation_escalations_operation_reason UNIQUE (operation_id, reason),
    CONSTRAINT fk_reconciliation_escalations_operation FOREIGN KEY (operation_id) REFERENCES payment_operations (id),
    CONSTRAINT fk_reconciliation_escalations_job FOREIGN KEY (reconciliation_job_id) REFERENCES reconciliation_jobs (id),
    CONSTRAINT ck_reconciliation_escalations_reason CHECK (reason IN ('PSP_UNKNOWN_24H', 'PSP_EVIDENCE_CONTRADICTION')),
    CONSTRAINT ck_reconciliation_escalations_status CHECK (status IN ('OPEN', 'CLOSED')),
    CONSTRAINT ck_reconciliation_escalations_status_resolved
        CHECK ((status = 'OPEN' AND resolved_at IS NULL) OR (status = 'CLOSED' AND resolved_at IS NOT NULL))
);

CREATE INDEX ix_reconciliation_escalations_open ON reconciliation_escalations (created_at) WHERE status = 'OPEN';

CREATE VIEW reconciliation_escalations_open AS
SELECT re.id, re.operation_id, po.type AS operation_type, po.status AS operation_status,
       p.id AS payment_id, p.status AS payment_status, re.reason, re.created_at,
       re.last_notification_at, rj.attempts, rj.last_error, rj.next_check_at
FROM reconciliation_escalations re
JOIN payment_operations po ON po.id = re.operation_id
JOIN payments p ON p.id = po.payment_id
JOIN reconciliation_jobs rj ON rj.id = re.reconciliation_job_id
WHERE re.status = 'OPEN';
