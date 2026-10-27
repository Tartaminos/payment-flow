ALTER TABLE idempotency_keys
    DROP CONSTRAINT ck_idempotency_keys_scope;

ALTER TABLE idempotency_keys
    ADD CONSTRAINT ck_idempotency_keys_scope
        CHECK (operation_scope IN ('PAYMENT_CREATE', 'PAYMENT_CAPTURE', 'PAYMENT_VOID'));
