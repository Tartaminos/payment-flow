package jhsoftware.paymentsflow.processor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class ScheduledRetryRepository {

	private final JdbcClient jdbcClient;
	private final RetryBackoffPolicy backoffPolicy;

	ScheduledRetryRepository(JdbcClient jdbcClient, RetryBackoffPolicy backoffPolicy) {
		this.jdbcClient = jdbcClient;
		this.backoffPolicy = backoffPolicy;
	}

	@Transactional
	ScheduledRetry persist(PaymentOperationRetryEnvelope envelope, int sourceSchemaId) {
		Optional<StoredRetry> existing = find(envelope.retryId());
		if (existing.isPresent()) {
			return verified(existing.get(), envelope, sourceSchemaId).schedule();
		}

		int retryNumber = resolveRetryNumber(envelope.sourceMessageId());
		Duration delay = backoffPolicy.delay(retryNumber);
		jdbcClient.sql("""
				INSERT INTO scheduled_retries
				    (retry_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, source_schema_id,
				     failure_class, failed_at, operation_id, correlation_id, source_message_id,
				     retry_number, scheduled_at, next_eligible_at, state,
				     claim_generation, created_at, updated_at)
				VALUES
				    (:retryId, :sourceTopic, :sourcePartition, :sourceOffset,
				     :sourceRecordKey, :sourceRecordValue, :sourceSchemaId,
				     :failureClass, :failedAt, :operationId, :correlationId, :sourceMessageId,
				     :retryNumber, transaction_timestamp(),
				     transaction_timestamp() + (:delayMillis * interval '1 millisecond'), 'PENDING',
				     0, transaction_timestamp(), transaction_timestamp())
				ON CONFLICT DO NOTHING
				""")
				.param("retryId", envelope.retryId())
				.param("sourceTopic", envelope.sourceTopic())
				.param("sourcePartition", envelope.sourcePartition())
				.param("sourceOffset", envelope.sourceOffset())
				.param("sourceRecordKey", envelope.sourceRecordKey())
				.param("sourceRecordValue", envelope.sourceRecordValue())
				.param("sourceSchemaId", sourceSchemaId)
				.param("failureClass", envelope.failureClass())
				.param("failedAt", OffsetDateTime.ofInstant(envelope.failedAt(), ZoneOffset.UTC))
				.param("operationId", envelope.operationId())
				.param("correlationId", envelope.correlationId())
				.param("sourceMessageId", envelope.sourceMessageId())
				.param("retryNumber", retryNumber)
				.param("delayMillis", delay.toMillis())
				.update();

		StoredRetry stored = find(envelope.retryId())
				.orElseThrow(() -> new IllegalStateException("Conflicting immutable retry source identity"));
		return verified(stored, envelope, sourceSchemaId).schedule();
	}

	private int resolveRetryNumber(UUID sourceMessageId) {
		if (sourceMessageId == null) {
			throw new UnresolvedRetryLineageException();
		}
		Optional<Integer> parent = jdbcClient.sql("""
				SELECT retry_number
				FROM scheduled_retries
				WHERE dispatch_message_id = :sourceMessageId
				""")
				.param("sourceMessageId", sourceMessageId)
				.query(Integer.class)
				.optional();
		if (parent.isPresent()) {
			return Math.addExact(parent.get(), 1);
		}
		boolean originalPublication = jdbcClient.sql("""
				SELECT EXISTS (
				    SELECT 1 FROM outbox_events WHERE message_id = :sourceMessageId
				)
				""")
				.param("sourceMessageId", sourceMessageId)
				.query(Boolean.class)
				.single();
		if (!originalPublication) {
			throw new UnresolvedRetryLineageException();
		}
		return 1;
	}

	private Optional<StoredRetry> find(UUID retryId) {
		return jdbcClient.sql("""
				SELECT retry_id, source_topic, source_partition, source_offset,
				       source_record_key, source_record_value, source_schema_id,
				       operation_id, correlation_id, source_message_id,
				       retry_number, scheduled_at, next_eligible_at
				FROM scheduled_retries
				WHERE retry_id = :retryId
				""")
				.param("retryId", retryId)
				.query((resultSet, rowNumber) -> stored(resultSet))
				.optional();
	}

	private StoredRetry verified(
			StoredRetry stored, PaymentOperationRetryEnvelope envelope, int sourceSchemaId) {
		if (!stored.matches(envelope, sourceSchemaId)) {
			throw new IllegalStateException("Conflicting immutable retry identity");
		}
		return stored;
	}

	private StoredRetry stored(ResultSet resultSet) throws SQLException {
		return new StoredRetry(
				resultSet.getObject("retry_id", UUID.class),
				resultSet.getString("source_topic"),
				resultSet.getInt("source_partition"),
				resultSet.getLong("source_offset"),
				resultSet.getBytes("source_record_key"),
				resultSet.getBytes("source_record_value"),
				resultSet.getInt("source_schema_id"),
				resultSet.getObject("operation_id", UUID.class),
				resultSet.getObject("correlation_id", UUID.class),
				resultSet.getObject("source_message_id", UUID.class),
				resultSet.getInt("retry_number"),
				resultSet.getObject("scheduled_at", OffsetDateTime.class).toInstant(),
				resultSet.getObject("next_eligible_at", OffsetDateTime.class).toInstant());
	}

	private record StoredRetry(
			UUID retryId,
			String sourceTopic,
			int sourcePartition,
			long sourceOffset,
			byte[] sourceRecordKey,
			byte[] sourceRecordValue,
			int sourceSchemaId,
			UUID operationId,
			UUID correlationId,
			UUID sourceMessageId,
			int retryNumber,
			java.time.Instant scheduledAt,
			java.time.Instant nextEligibleAt) {

		boolean matches(PaymentOperationRetryEnvelope envelope, int schemaId) {
			return retryId.equals(envelope.retryId())
					&& sourceTopic.equals(envelope.sourceTopic())
					&& sourcePartition == envelope.sourcePartition()
					&& sourceOffset == envelope.sourceOffset()
					&& Arrays.equals(sourceRecordKey, envelope.sourceRecordKey())
					&& Arrays.equals(sourceRecordValue, envelope.sourceRecordValue())
					&& sourceSchemaId == schemaId
					&& Objects.equals(operationId, envelope.operationId())
					&& Objects.equals(correlationId, envelope.correlationId())
					&& Objects.equals(sourceMessageId, envelope.sourceMessageId());
		}

		ScheduledRetry schedule() {
			return new ScheduledRetry(retryId, retryNumber, scheduledAt, nextEligibleAt);
		}
	}
}
