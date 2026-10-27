ALTER TABLE psp_operations
    ADD COLUMN operation_type VARCHAR(20) NOT NULL DEFAULT 'AUTHORIZE',
    ADD COLUMN authorization_psp_operation_id UUID,
    ADD COLUMN validity VARCHAR(20),
    ADD COLUMN validity_evidence_at TIMESTAMPTZ;

ALTER TABLE psp_operations
    ADD CONSTRAINT ck_psp_operations_operation_type CHECK (operation_type IN ('AUTHORIZE', 'CAPTURE', 'VOID')),
    ADD CONSTRAINT ck_psp_operations_validity CHECK (validity IN ('ACTIVE', 'EXPIRED'));

UPDATE psp_operations
SET validity = 'ACTIVE', validity_evidence_at = created_at
WHERE validity IS NULL;
