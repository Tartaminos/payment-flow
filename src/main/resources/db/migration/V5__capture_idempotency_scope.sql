ALTER TABLE idempotency_keys
    DROP CONSTRAINT ck_idempotency_keys_scope;

ALTER TABLE idempotency_keys
    ADD CONSTRAINT ck_idempotency_keys_scope
        CHECK (operation_scope IN ('PAYMENT_CREATE', 'PAYMENT_CAPTURE'));

CREATE UNIQUE INDEX uk_payment_operations_active_capture_void
    ON payment_operations (payment_id)
    WHERE type IN ('CAPTURE', 'VOID') AND status IN ('PENDING', 'PROCESSING', 'UNKNOWN');
