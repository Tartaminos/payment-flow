package jhsoftware.paymentsflow.retention;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
class RetentionCleanupRepository {

	private final JdbcClient jdbcClient;

	RetentionCleanupRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	List<UUID> findCleanupCandidatePaymentIds(Instant now, Period retention) {
		OffsetDateTime cutoff = now.atOffset(ZoneOffset.UTC).minus(retention);
		return jdbcClient.sql("""
				SELECT p.id
				FROM payments p
				WHERE p.id IN (
				    SELECT payment_id
				    FROM payment_audit_events
				    GROUP BY payment_id
				    HAVING MAX(recorded_at) < :cutoff
				)
				AND NOT EXISTS (
				    SELECT 1 FROM payment_operations po
				    WHERE po.payment_id = p.id AND po.status = 'UNKNOWN'
				)
				AND NOT EXISTS (
				    SELECT 1 FROM reconciliation_jobs rj
				    JOIN payment_operations po ON po.id = rj.operation_id
				    WHERE po.payment_id = p.id AND rj.resolved_at IS NULL
				)
				AND NOT EXISTS (
				    SELECT 1 FROM reconciliation_escalations re
				    JOIN payment_operations po ON po.id = re.operation_id
				    WHERE po.payment_id = p.id AND re.status = 'OPEN'
				)
				AND NOT EXISTS (
				    SELECT 1 FROM scheduled_retries sr
				    JOIN payment_operations po ON po.id = sr.operation_id
				    WHERE po.payment_id = p.id AND sr.state IN ('PENDING', 'CLAIMED', 'DISPATCHED')
				)
				AND NOT EXISTS (
				    SELECT 1 FROM quarantined_commands qc
				    JOIN payment_operations po ON po.id = qc.operation_id
				    WHERE po.payment_id = p.id
				)
				""")
				.param("cutoff", cutoff)
				.query((resultSet, rowNumber) -> resultSet.getObject("id", UUID.class))
				.list();
	}

	@Transactional
	int deleteCleanupCandidates(List<UUID> paymentIds) {
		if (paymentIds.isEmpty()) {
			return 0;
		}

		jdbcClient.sql("""
				DELETE FROM reconciliation_escalations
				WHERE operation_id IN (
				    SELECT po.id FROM payment_operations po WHERE po.payment_id IN (:paymentIds)
				)
				""")
				.param("paymentIds", paymentIds)
				.update();

		jdbcClient.sql("""
				DELETE FROM reconciliation_jobs
				WHERE operation_id IN (
				    SELECT po.id FROM payment_operations po WHERE po.payment_id IN (:paymentIds)
				)
				""")
				.param("paymentIds", paymentIds)
				.update();

		jdbcClient.sql("""
				DELETE FROM outbox_events
				WHERE payment_id IN (:paymentIds)
				""")
				.param("paymentIds", paymentIds)
				.update();

		return jdbcClient.sql("""
				DELETE FROM payment_audit_events
				WHERE payment_id IN (:paymentIds)
				""")
				.param("paymentIds", paymentIds)
				.update();
	}
}
