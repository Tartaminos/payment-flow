package jhsoftware.paymentsflow.processor.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class ReconciliationEscalationRepository {

	private final JdbcClient jdbcClient;

	ReconciliationEscalationRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	@Transactional
	public boolean ensureEscalation(UUID operationId, UUID jobId, String reason, Instant now) {
		return jdbcClient.sql("""
				INSERT INTO reconciliation_escalations
				    (id, operation_id, reconciliation_job_id, reason, status, created_at, updated_at)
				VALUES
				    (:id, :operationId, :jobId, :reason, 'OPEN', :now, :now)
				ON CONFLICT (operation_id, reason) DO NOTHING
				""")
				.param("id", UUID.randomUUID())
				.param("operationId", operationId)
				.param("jobId", jobId)
				.param("reason", reason)
				.param("now", databaseTimestamp(now))
				.update() == 1;
	}

	@Transactional
	List<String> closeOpenEscalations(UUID operationId, Instant now) {
		return jdbcClient.sql("""
				UPDATE reconciliation_escalations
				SET status = 'CLOSED',
				    resolved_at = :now,
				    updated_at = :now
				WHERE operation_id = :operationId
				  AND status = 'OPEN'
				RETURNING reason
				""")
				.param("operationId", operationId)
				.param("now", databaseTimestamp(now))
				.query(String.class)
				.list();
	}

	private OffsetDateTime databaseTimestamp(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}
}
