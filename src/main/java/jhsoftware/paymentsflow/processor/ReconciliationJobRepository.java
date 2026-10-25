package jhsoftware.paymentsflow.processor;

import java.time.Duration;
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
class ReconciliationJobRepository {

	private final JdbcClient jdbcClient;
	private final PaymentOperationProcessingRepository operationRepository;

	ReconciliationJobRepository(JdbcClient jdbcClient, PaymentOperationProcessingRepository operationRepository) {
		this.jdbcClient = jdbcClient;
		this.operationRepository = operationRepository;
	}

	@Transactional
	List<ReconciliationJobClaim> claimDueJobs(UUID self, Instant now, int batchSize, Duration claimLease) {
		return jdbcClient.sql("""
				UPDATE reconciliation_jobs
				SET lease_owner = :self,
				    lease_until = :leaseUntil,
				    updated_at = :now
				WHERE id IN (
				    SELECT id FROM reconciliation_jobs
				    WHERE resolved_at IS NULL
				      AND next_check_at <= :now
				      AND (lease_until IS NULL OR lease_until < :now)
				    ORDER BY next_check_at, id
				    LIMIT :batchSize
				    FOR UPDATE SKIP LOCKED
				)
				RETURNING id, operation_id, attempts
				""")
				.param("self", self)
				.param("leaseUntil", databaseTimestamp(now.plus(claimLease)))
				.param("now", databaseTimestamp(now))
				.param("batchSize", batchSize)
				.query((resultSet, rowNumber) -> new ReconciliationJobClaim(
						resultSet.getObject("id", UUID.class),
						resultSet.getObject("operation_id", UUID.class),
						resultSet.getInt("attempts")))
				.list();
	}

	@Transactional
	boolean resolveJob(
			UUID jobId, UUID self, UUID operationId, PspAuthorizationOutcome outcome,
			UUID correlationId, UUID causationId, Instant now) {
		boolean applied = operationRepository.applyDefinitiveOutcome(operationId, outcome, correlationId, causationId, now);
		if (!applied) {
			return false;
		}

		return jdbcClient.sql("""
				UPDATE reconciliation_jobs
				SET resolved_at = :now,
				    lease_owner = NULL,
				    lease_until = NULL,
				    updated_at = :now
				WHERE id = :jobId
				  AND lease_owner = :self
				""")
				.param("now", databaseTimestamp(now))
				.param("jobId", jobId)
				.param("self", self)
				.update() == 1;
	}

	@Transactional
	boolean bumpBackoff(UUID jobId, UUID self, String reason, Instant now, Instant nextCheckAt) {
		return jdbcClient.sql("""
				UPDATE reconciliation_jobs
				SET attempts = attempts + 1,
				    last_error = :reason,
				    next_check_at = :nextCheckAt,
				    lease_owner = NULL,
				    lease_until = NULL,
				    updated_at = :now
				WHERE id = :jobId
				  AND lease_owner = :self
				""")
				.param("reason", reason)
				.param("nextCheckAt", databaseTimestamp(nextCheckAt))
				.param("now", databaseTimestamp(now))
				.param("jobId", jobId)
				.param("self", self)
				.update() == 1;
	}

	private OffsetDateTime databaseTimestamp(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

	record ReconciliationJobClaim(UUID jobId, UUID operationId, int attempts) {
	}
}
