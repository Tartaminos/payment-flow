package jhsoftware.paymentsflow.outbox;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class OutboxPublicationRepository {

	private final JdbcClient jdbcClient;

	OutboxPublicationRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	List<OutboxPublication> claimPending(int batchMax, UUID workerId, Instant now, Instant leaseExpiresAt) {
		List<UUID> claimedIds = jdbcClient.sql("""
				UPDATE outbox_events
				SET status = 'PUBLISHING',
				    claimed_by = :workerId,
				    claimed_at = :now,
				    lease_expires_at = :leaseExpiresAt,
				    attempt_count = attempt_count + 1
				WHERE message_id IN (
				    SELECT message_id FROM outbox_events
				    WHERE (status = 'PENDING' AND (next_eligible_at IS NULL OR next_eligible_at <= :now))
				       OR (status = 'PUBLISHING' AND lease_expires_at < :now)
				    ORDER BY created_at, message_id
				    LIMIT :batchMax
				    FOR UPDATE SKIP LOCKED
				)
				RETURNING message_id
				""")
				.param("workerId", workerId)
				.param("now", now.atOffset(ZoneOffset.UTC))
				.param("leaseExpiresAt", leaseExpiresAt.atOffset(ZoneOffset.UTC))
				.param("batchMax", batchMax)
				.query((resultSet, rowNumber) -> resultSet.getObject("message_id", UUID.class))
				.list();

		if (claimedIds.isEmpty()) {
			return List.of();
		}

		return jdbcClient.sql("""
				SELECT oe.message_id,
				       oe.payment_id,
				       oe.operation_id,
				       po.payment_id AS authoritative_payment_id,
				       oe.command_type,
				       oe.correlation_id,
				       oe.causation_id,
				       oe.occurred_at,
				       oe.attempt_count
				FROM outbox_events oe
				LEFT JOIN payment_operations po ON po.id = oe.operation_id
				WHERE oe.message_id IN (:claimedIds)
				  AND oe.claimed_by = :workerId
				ORDER BY oe.created_at, oe.message_id
				""")
				.param("claimedIds", claimedIds)
				.param("workerId", workerId)
				.query((resultSet, rowNumber) -> new OutboxPublication(
						resultSet.getObject("message_id", UUID.class),
						resultSet.getObject("payment_id", UUID.class),
						resultSet.getObject("operation_id", UUID.class),
						resultSet.getObject("authoritative_payment_id", UUID.class),
						resultSet.getString("command_type"),
						resultSet.getObject("correlation_id", UUID.class),
						resultSet.getObject("causation_id", UUID.class),
						resultSet.getObject("occurred_at", OffsetDateTime.class).toInstant(),
						resultSet.getInt("attempt_count")))
				.list();
	}

	boolean markPublished(UUID messageId, UUID workerId, Instant publishedAt) {
		return jdbcClient.sql("""
				UPDATE outbox_events
				SET status = 'PUBLISHED',
				    published_at = :publishedAt,
				    claimed_by = NULL,
				    claimed_at = NULL,
				    lease_expires_at = NULL,
				    next_eligible_at = NULL
				WHERE message_id = :messageId
				  AND status = 'PUBLISHING'
				  AND claimed_by = :workerId
				""")
				.param("messageId", messageId)
				.param("workerId", workerId)
				.param("publishedAt", publishedAt.atOffset(ZoneOffset.UTC))
				.update() == 1;
	}

	boolean releaseForRetry(UUID messageId, UUID workerId, Instant nextEligibleAt) {
		return jdbcClient.sql("""
				UPDATE outbox_events
				SET status = 'PENDING',
				    claimed_by = NULL,
				    claimed_at = NULL,
				    lease_expires_at = NULL,
				    next_eligible_at = :nextEligibleAt
				WHERE message_id = :messageId
				  AND status = 'PUBLISHING'
				  AND claimed_by = :workerId
				""")
				.param("messageId", messageId)
				.param("workerId", workerId)
				.param("nextEligibleAt", nextEligibleAt.atOffset(ZoneOffset.UTC))
				.update() == 1;
	}
}
