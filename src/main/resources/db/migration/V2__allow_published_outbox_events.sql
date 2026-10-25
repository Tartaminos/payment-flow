ALTER TABLE outbox_events
    DROP CONSTRAINT ck_outbox_events_status,
    DROP CONSTRAINT ck_outbox_events_unpublished;

ALTER TABLE outbox_events
    ADD CONSTRAINT ck_outbox_events_status
        CHECK (status IN ('PENDING', 'PUBLISHED')),
    ADD CONSTRAINT ck_outbox_events_delivery_state
        CHECK (
            (status = 'PENDING' AND published_at IS NULL)
            OR (status = 'PUBLISHED' AND published_at IS NOT NULL)
        );
