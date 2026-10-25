CREATE TABLE psp_operations (
    id UUID NOT NULL,
    merchant_id UUID NOT NULL,
    psp_operation_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    decision VARCHAR(20) NOT NULL,
    decision_reason VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_psp_operations PRIMARY KEY (id),
    CONSTRAINT uk_psp_operations_merchant_psp_operation UNIQUE (merchant_id, psp_operation_id),
    CONSTRAINT ck_psp_operations_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT ck_psp_operations_currency CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_psp_operations_decision CHECK (decision IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'PROCESSING'))
);

CREATE INDEX ix_psp_operations_psp_operation_id ON psp_operations (psp_operation_id);
