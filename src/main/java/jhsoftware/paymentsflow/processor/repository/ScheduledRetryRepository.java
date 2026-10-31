package jhsoftware.paymentsflow.processor.repository;

import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.LostScheduledRetryClaimException;
import jhsoftware.paymentsflow.processor.exception.UnresolvedRetryLineageException;
import jhsoftware.paymentsflow.processor.model.DueScheduledRetry;
import jhsoftware.paymentsflow.processor.model.ScheduledRetry;
import jhsoftware.paymentsflow.processor.service.RetryBackoffPolicy;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class ScheduledRetryRepository {

	private static final Logger logger = LoggerFactory.getLogger(ScheduledRetryRepository.class);

	private final JdbcClient jdbcClient;
	private final RetryBackoffPolicy backoffPolicy;

	public ScheduledRetryRepository(JdbcClient jdbcClient, RetryBackoffPolicy backoffPolicy) {
		this.jdbcClient = jdbcClient;
		this.backoffPolicy = backoffPolicy;
	}

	public BacklogStats backlogStats(Instant now) {
		return jdbcClient.sql("""
				SELECT COUNT(*) AS backlog_count,
				       COALESCE(MIN(created_at), :now) AS oldest_pending_at
				FROM scheduled_retries
				WHERE state IN ('PENDING', 'CLAIMED', 'DISPATCHED')
				""")
				.param("now", now.atOffset(ZoneOffset.UTC))
				.query((resultSet, rowNumber) -> new BacklogStats(
						resultSet.getLong("backlog_count"),
						Duration.between(
								resultSet.getObject("oldest_pending_at", OffsetDateTime.class).toInstant(), now)
								.getSeconds()))
				.single();
	}

	public record BacklogStats(long backlogCount, long oldestPendingAgeSeconds) {
	}

	@Transactional
	public List<DueScheduledRetry> claimDue(int batchMax, UUID workerId, Instant now, Instant leaseExpiresAt) {
		List<UUID> claimedIds = jdbcClient.sql("""
				UPDATE scheduled_retries
				SET state = 'CLAIMED', claim_owner = :workerId, claimed_at = :now,
				    claim_lease_expires_at = :leaseExpiresAt, claim_generation = claim_generation + 1
				WHERE retry_id IN (
				    SELECT retry_id FROM scheduled_retries
				    WHERE operation_id IS NOT NULL AND correlation_id IS NOT NULL
				      AND ((state = 'PENDING' AND next_eligible_at <= :now)
				        OR (state = 'CLAIMED' AND claim_lease_expires_at < :now))
				    ORDER BY next_eligible_at, retry_id
				    LIMIT :batchMax
				    FOR UPDATE SKIP LOCKED)
				RETURNING retry_id
				""")
				.param("workerId", workerId)
				.param("now", now.atOffset(ZoneOffset.UTC))
				.param("leaseExpiresAt", leaseExpiresAt.atOffset(ZoneOffset.UTC))
				.param("batchMax", batchMax)
				.query((resultSet, rowNumber) -> resultSet.getObject("retry_id", UUID.class))
				.list();

		if (claimedIds.isEmpty()) {
			return List.of();
		}

		List<DueScheduledRetry> claimed = jdbcClient.sql("""
				SELECT sr.retry_id, sr.operation_id, sr.correlation_id, sr.source_message_id,
				       sr.dispatch_message_id, po.payment_id AS payment_id
				FROM scheduled_retries sr
				LEFT JOIN payment_operations po ON po.id = sr.operation_id
				WHERE sr.retry_id IN (:claimedIds)
				  AND sr.claim_owner = :workerId
				  AND sr.state = 'CLAIMED'
				ORDER BY sr.next_eligible_at, sr.retry_id
				""")
				.param("claimedIds", claimedIds)
				.param("workerId", workerId)
				.query((resultSet, rowNumber) -> new DueScheduledRetry(
						resultSet.getObject("retry_id", UUID.class),
						resultSet.getObject("operation_id", UUID.class),
						resultSet.getObject("correlation_id", UUID.class),
						resultSet.getObject("source_message_id", UUID.class),
						resultSet.getObject("dispatch_message_id", UUID.class),
						resultSet.getObject("payment_id", UUID.class)))
				.list();

		List<DueScheduledRetry> withResolvedPayment = new ArrayList<>();
		for (DueScheduledRetry retry : claimed) {
			if (retry.paymentId() == null) {
				logger.error(
						"Scheduled retry {} references operation {} with no matching payment_operations row",
						retry.retryId(), retry.operationId());
				continue;
			}
			withResolvedPayment.add(retry);
		}
		return withResolvedPayment;
	}

	@Transactional
	public void dispatch(DueScheduledRetry claimed, UUID workerId, Instant now) {
		UUID messageId = claimed.dispatchMessageId() != null ? claimed.dispatchMessageId() : UUID.randomUUID();

		jdbcClient.sql("""
				INSERT INTO outbox_events
				    (message_id, payment_id, operation_id, command_type, correlation_id, causation_id,
				     occurred_at, status, created_at, published_at)
				VALUES
				    (:messageId, :paymentId, :operationId, 'ExecutePaymentOperation', :correlationId, :causationId,
				     :now, 'PENDING', :now, NULL)
				""")
				.param("messageId", messageId)
				.param("paymentId", claimed.paymentId())
				.param("operationId", claimed.operationId())
				.param("correlationId", claimed.correlationId())
				.param("causationId", claimed.sourceMessageId())
				.param("now", now.atOffset(ZoneOffset.UTC))
				.update();

		boolean marked = jdbcClient.sql("""
				UPDATE scheduled_retries
				SET state = 'DISPATCHED', dispatch_message_id = :messageId, updated_at = :now
				WHERE retry_id = :retryId AND claim_owner = :workerId AND state = 'CLAIMED'
				""")
				.param("messageId", messageId)
				.param("now", now.atOffset(ZoneOffset.UTC))
				.param("retryId", claimed.retryId())
				.param("workerId", workerId)
				.update() == 1;

		if (!marked) {
			throw new LostScheduledRetryClaimException(claimed.retryId());
		}
	}

	@Transactional
	public ScheduledRetry persist(PaymentOperationRetryEnvelopeDto envelope, int sourceSchemaId) {
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
			StoredRetry stored, PaymentOperationRetryEnvelopeDto envelope, int sourceSchemaId) {
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

		boolean matches(PaymentOperationRetryEnvelopeDto envelope, int schemaId) {
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
