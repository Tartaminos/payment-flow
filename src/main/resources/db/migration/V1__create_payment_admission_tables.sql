CREATE TABLE payments (
    id UUID NOT NULL,
    merchant_id UUID NOT NULL,
    external_order_id VARCHAR(100) NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    status VARCHAR(30) NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_payments PRIMARY KEY (id),
    CONSTRAINT ck_payments_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT ck_payments_currency CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_payments_status CHECK (status IN ('CREATED', 'AUTHORIZED', 'CAPTURED', 'DECLINED', 'CANCELED', 'EXPIRED')),
    CONSTRAINT ck_payments_version_nonnegative CHECK (version >= 0)
);

CREATE INDEX ix_payments_merchant_order ON payments (merchant_id, external_order_id);

CREATE TABLE payment_operations (
    id UUID NOT NULL,
    payment_id UUID NOT NULL,
    type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    amount_minor BIGINT NOT NULL,
    psp_idempotency_key VARCHAR(150) NOT NULL,
    psp_reference VARCHAR(150),
    psp_result_code VARCHAR(100),
    error_code VARCHAR(100),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    unknown_since TIMESTAMPTZ,
    authorization_expires_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_payment_operations PRIMARY KEY (id),
    CONSTRAINT fk_payment_operations_payment FOREIGN KEY (payment_id) REFERENCES payments (id),
    CONSTRAINT uk_payment_operations_psp_idempotency UNIQUE (psp_idempotency_key),
    CONSTRAINT ck_payment_operations_type CHECK (type IN ('AUTHORIZE', 'CAPTURE', 'VOID')),
    CONSTRAINT ck_payment_operations_status CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCEEDED', 'REJECTED', 'FAILED', 'UNKNOWN')),
    CONSTRAINT ck_payment_operations_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT ck_payment_operations_attempts_nonnegative CHECK (attempt_count >= 0)
);

CREATE INDEX ix_payment_operations_payment ON payment_operations (payment_id);

CREATE TABLE idempotency_keys (
    id UUID NOT NULL,
    merchant_id UUID NOT NULL,
    operation_scope VARCHAR(50) NOT NULL,
    key VARCHAR(150) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    payment_id UUID NOT NULL,
    payment_operation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    CONSTRAINT pk_idempotency_keys PRIMARY KEY (id),
    CONSTRAINT fk_idempotency_keys_payment FOREIGN KEY (payment_id) REFERENCES payments (id),
    CONSTRAINT fk_idempotency_keys_operation FOREIGN KEY (payment_operation_id) REFERENCES payment_operations (id),
    CONSTRAINT uk_idempotency_keys_scope UNIQUE (merchant_id, operation_scope, key),
    CONSTRAINT ck_idempotency_keys_scope CHECK (operation_scope = 'PAYMENT_CREATE'),
    CONSTRAINT ck_idempotency_keys_hash CHECK (request_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX ix_idempotency_keys_payment ON idempotency_keys (payment_id);

CREATE TABLE payment_audit_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    event_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    payment_operation_id UUID,
    entity_type VARCHAR(30) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    previous_status VARCHAR(30),
    new_status VARCHAR(30),
    reason_code VARCHAR(100),
    source VARCHAR(50) NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    sequence_number BIGINT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_payment_audit_events PRIMARY KEY (id),
    CONSTRAINT uk_payment_audit_events_event UNIQUE (event_id),
    CONSTRAINT uk_payment_audit_events_sequence UNIQUE (payment_id, sequence_number),
    CONSTRAINT fk_payment_audit_events_payment FOREIGN KEY (payment_id) REFERENCES payments (id),
    CONSTRAINT fk_payment_audit_events_operation FOREIGN KEY (payment_operation_id) REFERENCES payment_operations (id),
    CONSTRAINT ck_payment_audit_events_entity CHECK (entity_type IN ('PAYMENT', 'PAYMENT_OPERATION')),
    CONSTRAINT ck_payment_audit_events_sequence_positive CHECK (sequence_number > 0)
);

CREATE INDEX ix_payment_audit_events_operation ON payment_audit_events (payment_operation_id);

CREATE TABLE outbox_events (
    message_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    operation_id UUID NOT NULL,
    command_type VARCHAR(100) NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT pk_outbox_events PRIMARY KEY (message_id),
    CONSTRAINT fk_outbox_events_payment FOREIGN KEY (payment_id) REFERENCES payments (id),
    CONSTRAINT fk_outbox_events_operation FOREIGN KEY (operation_id) REFERENCES payment_operations (id),
    CONSTRAINT ck_outbox_events_command_type CHECK (command_type = 'ExecutePaymentOperation'),
    CONSTRAINT ck_outbox_events_status CHECK (status = 'PENDING'),
    CONSTRAINT ck_outbox_events_unpublished CHECK (published_at IS NULL)
);

CREATE INDEX ix_outbox_events_pending ON outbox_events (status, created_at);
CREATE INDEX ix_outbox_events_payment ON outbox_events (payment_id);
