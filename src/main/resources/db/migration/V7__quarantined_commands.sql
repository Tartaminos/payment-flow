CREATE TABLE quarantined_commands (
    quarantine_id UUID NOT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    source_record_key BYTEA NOT NULL,
    source_record_value BYTEA NOT NULL,
    failure_code VARCHAR(64) NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL,
    operation_id UUID,
    correlation_id UUID,
    source_message_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_quarantined_commands PRIMARY KEY (quarantine_id),
    CONSTRAINT uk_quarantined_commands_source UNIQUE (source_topic, source_partition, source_offset),
    CONSTRAINT ck_quarantined_commands_partition_nonnegative CHECK (source_partition >= 0),
    CONSTRAINT ck_quarantined_commands_offset_nonnegative CHECK (source_offset >= 0)
);

CREATE INDEX ix_quarantined_commands_operation ON quarantined_commands (operation_id);
CREATE INDEX ix_quarantined_commands_correlation ON quarantined_commands (correlation_id);
CREATE INDEX ix_quarantined_commands_failure_time ON quarantined_commands (failure_code, quarantined_at);
