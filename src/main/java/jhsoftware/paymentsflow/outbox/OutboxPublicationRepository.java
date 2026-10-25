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

	List<OutboxPublication> findPending(int batchMax) {
		return jdbcClient.sql("""
				SELECT oe.message_id,
				       oe.payment_id,
				       oe.operation_id,
				       po.payment_id AS authoritative_payment_id,
				       oe.command_type,
				       oe.correlation_id,
				       oe.causation_id,
				       oe.occurred_at
				FROM outbox_events oe
				LEFT JOIN payment_operations po ON po.id = oe.operation_id
				WHERE oe.status = 'PENDING'
				ORDER BY oe.created_at, oe.message_id
				LIMIT :batchMax
				""")
				.param("batchMax", batchMax)
				.query((resultSet, rowNumber) -> new OutboxPublication(
						resultSet.getObject("message_id", UUID.class),
						resultSet.getObject("payment_id", UUID.class),
						resultSet.getObject("operation_id", UUID.class),
						resultSet.getObject("authoritative_payment_id", UUID.class),
						resultSet.getString("command_type"),
						resultSet.getObject("correlation_id", UUID.class),
						resultSet.getObject("causation_id", UUID.class),
						resultSet.getObject("occurred_at", OffsetDateTime.class).toInstant()))
				.list();
	}

	boolean markPublished(UUID messageId, Instant publishedAt) {
		return jdbcClient.sql("""
				UPDATE outbox_events
				SET status = 'PUBLISHED', published_at = :publishedAt
				WHERE message_id = :messageId
				  AND status = 'PENDING'
				""")
				.param("messageId", messageId)
				.param("publishedAt", publishedAt.atOffset(ZoneOffset.UTC))
				.update() == 1;
	}
}
