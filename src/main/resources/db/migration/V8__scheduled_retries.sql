CREATE TABLE scheduled_retries (
    retry_id UUID NOT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    source_record_key BYTEA NOT NULL,
    source_record_value BYTEA NOT NULL,
    source_schema_id INTEGER NOT NULL,
    failure_class VARCHAR(64) NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    operation_id UUID,
    correlation_id UUID,
    source_message_id UUID,
    retry_number INTEGER NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    next_eligible_at TIMESTAMPTZ NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    claim_owner UUID,
    claimed_at TIMESTAMPTZ,
    claim_lease_expires_at TIMESTAMPTZ,
    claim_generation BIGINT NOT NULL DEFAULT 0,
    dispatch_message_id UUID,
    last_error VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_scheduled_retries PRIMARY KEY (retry_id),
    CONSTRAINT uk_scheduled_retries_source UNIQUE (source_topic, source_partition, source_offset),
    CONSTRAINT ck_scheduled_retries_partition_nonnegative CHECK (source_partition >= 0),
    CONSTRAINT ck_scheduled_retries_offset_nonnegative CHECK (source_offset >= 0),
    CONSTRAINT ck_scheduled_retries_schema_id_nonnegative CHECK (source_schema_id >= 0),
    CONSTRAINT ck_scheduled_retries_retry_number_positive CHECK (retry_number > 0),
    CONSTRAINT ck_scheduled_retries_state CHECK (state IN ('PENDING', 'CLAIMED', 'DISPATCHED')),
    CONSTRAINT ck_scheduled_retries_claim_generation_nonnegative CHECK (claim_generation >= 0),
    CONSTRAINT ck_scheduled_retries_time_order CHECK (next_eligible_at >= scheduled_at)
);

CREATE INDEX ix_scheduled_retries_due ON scheduled_retries (state, next_eligible_at);
CREATE INDEX ix_scheduled_retries_operation ON scheduled_retries (operation_id);
CREATE INDEX ix_scheduled_retries_correlation ON scheduled_retries (correlation_id);
CREATE INDEX ix_scheduled_retries_source_message ON scheduled_retries (source_message_id);
CREATE UNIQUE INDEX uk_scheduled_retries_dispatch_message
    ON scheduled_retries (dispatch_message_id)
    WHERE dispatch_message_id IS NOT NULL;
