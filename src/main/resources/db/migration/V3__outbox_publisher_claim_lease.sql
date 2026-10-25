ALTER TABLE outbox_events
    DROP CONSTRAINT ck_outbox_events_status,
    DROP CONSTRAINT ck_outbox_events_delivery_state;

ALTER TABLE outbox_events
    ADD COLUMN claimed_by UUID,
    ADD COLUMN claimed_at TIMESTAMPTZ,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_eligible_at TIMESTAMPTZ;

ALTER TABLE outbox_events
    ADD CONSTRAINT ck_outbox_events_status
        CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED')),
    ADD CONSTRAINT ck_outbox_events_delivery_state
        CHECK (
            (status = 'PENDING' AND published_at IS NULL AND claimed_by IS NULL
                AND claimed_at IS NULL AND lease_expires_at IS NULL)
            OR (status = 'PUBLISHING' AND published_at IS NULL AND claimed_by IS NOT NULL
                AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (status = 'PUBLISHED' AND published_at IS NOT NULL)
        ),
    ADD CONSTRAINT ck_outbox_events_attempt_count_nonnegative CHECK (attempt_count >= 0);

DROP INDEX ix_outbox_events_pending;
CREATE INDEX ix_outbox_events_eligible ON outbox_events (status, lease_expires_at, next_eligible_at);
