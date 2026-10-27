package jhsoftware.paymentsflow.processor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationProcessingRepository {

	private static final String AUTHORIZATION_EXPIRED_REASON = "AUTHORIZATION_EXPIRED";
	private static final String CONTRADICTION_EVENT_TYPE = "PSP_EVIDENCE_CONTRADICTION";

	private final JdbcClient jdbcClient;

	PaymentOperationProcessingRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	Optional<PaymentOperationContext> loadContext(UUID operationId) {
		return jdbcClient.sql("""
				SELECT po.id AS operation_id,
				       po.type AS operation_type,
				       po.status AS operation_status,
				       po.amount_minor AS amount_minor,
				       po.attempt_count AS attempt_count,
				       po.lease_owner AS lease_owner,
				       p.id AS payment_id,
				       p.merchant_id AS merchant_id,
				       p.currency AS currency,
				       p.status AS payment_status,
				       authorize_op.psp_reference AS authorization_psp_reference,
				       EXISTS (
				           SELECT 1 FROM reconciliation_jobs rj
				           WHERE rj.operation_id = po.id AND rj.resolved_at IS NULL
				       ) AS has_unresolved_job
				FROM payment_operations po
				JOIN payments p ON p.id = po.payment_id
				LEFT JOIN payment_operations authorize_op
				    ON authorize_op.payment_id = p.id AND authorize_op.type = 'AUTHORIZE'
				WHERE po.id = :operationId
				""")
				.param("operationId", operationId)
				.query((resultSet, rowNumber) -> toContext(resultSet))
				.optional();
	}

	@Transactional
	LeaseClaim claimOrRenewLease(UUID operationId, UUID self, Instant now, Duration leaseDuration) {
		Optional<LeaseRow> rowOptional = jdbcClient.sql("""
				SELECT status, lease_owner, lease_until, attempt_count
				FROM payment_operations
				WHERE id = :operationId
				FOR UPDATE
				""")
				.param("operationId", operationId)
				.query((resultSet, rowNumber) -> new LeaseRow(
						resultSet.getString("status"),
						resultSet.getObject("lease_owner", UUID.class),
						resultSet.getObject("lease_until", OffsetDateTime.class),
						resultSet.getInt("attempt_count")))
				.optional();

		if (rowOptional.isEmpty()) {
			return new LeaseClaim(LeaseClaim.Status.NOT_FOUND, 0, null);
		}

		LeaseRow row = rowOptional.get();
		if (isFinalizedStatus(row.status())) {
			return new LeaseClaim(LeaseClaim.Status.FINALIZED, row.attemptCount(), row.leaseOwner());
		}

		boolean leaseAvailable = row.leaseUntil() == null
				|| row.leaseUntil().toInstant().isBefore(now)
				|| self.equals(row.leaseOwner());
		if (!leaseAvailable) {
			return new LeaseClaim(LeaseClaim.Status.DENIED, row.attemptCount(), row.leaseOwner());
		}

		jdbcClient.sql("""
				UPDATE payment_operations
				SET lease_owner = :self,
				    lease_until = :leaseUntil,
				    updated_at = :now
				WHERE id = :operationId
				""")
				.param("self", self)
				.param("leaseUntil", databaseTimestamp(now.plus(leaseDuration)))
				.param("now", databaseTimestamp(now))
				.param("operationId", operationId)
				.update();

		return new LeaseClaim(LeaseClaim.Status.ACQUIRED, row.attemptCount(), row.leaseOwner());
	}

	@Transactional
	Optional<Integer> reserveAttempt(UUID operationId, UUID self, Instant now, Duration leaseDuration) {
		return jdbcClient.sql("""
				UPDATE payment_operations
				SET attempt_count = attempt_count + 1,
				    last_attempt_at = :now,
				    lease_owner = :self,
				    lease_until = :leaseUntil,
				    updated_at = :now
				WHERE id = :operationId
				  AND lease_owner = :self
				  AND lease_until > :now
				  AND attempt_count < 3
				RETURNING attempt_count
				""")
				.param("operationId", operationId)
				.param("self", self)
				.param("now", databaseTimestamp(now))
				.param("leaseUntil", databaseTimestamp(now.plus(leaseDuration)))
				.query(Integer.class)
				.optional();
	}

	@Transactional
	boolean commitOutcome(
			UUID operationId, UUID self, PspAuthorizationOutcome outcome, UUID correlationId, UUID causationId, Instant now) {
		Optional<Locked> lockedOptional = lockOperation(operationId);
		if (lockedOptional.isEmpty()) {
			return false;
		}

		Locked locked = lockedOptional.get();
		if (isFinalizedStatus(locked.operationStatus())) {
			recordFinalizedEvidence(locked, outcome, correlationId, causationId, now);
			return true;
		}

		return applyOutcome(locked, outcome, correlationId, causationId, now, self)
				.map(OutcomeMapping::ackEligible)
				.orElse(false);
	}

	DefinitiveResolution applyDefinitiveOutcome(
			UUID operationId, UUID self, PspAuthorizationOutcome outcome, UUID correlationId, UUID causationId, Instant now) {
		Optional<Locked> lockedOptional = lockOperation(operationId);
		if (lockedOptional.isEmpty()) {
			return DefinitiveResolution.LEASE_LOST;
		}

		Locked locked = lockedOptional.get();
		if (isFinalizedStatus(locked.operationStatus())) {
			boolean contradiction = recordFinalizedEvidence(locked, outcome, correlationId, causationId, now);
			return contradiction ? DefinitiveResolution.CONTRADICTION : DefinitiveResolution.DUPLICATE;
		}

		return applyOutcome(locked, outcome, correlationId, causationId, now, self).isPresent()
				? DefinitiveResolution.APPLIED
				: DefinitiveResolution.LEASE_LOST;
	}

	private boolean recordFinalizedEvidence(
			Locked locked, PspAuthorizationOutcome outcome, UUID correlationId, UUID causationId, Instant now) {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(locked.operationType(), outcome, now);
		if (!isContradiction(locked, outcome, mapping)) {
			return false;
		}
		if (isDuplicateContradictionEvidence(locked.operationId(), mapping.newOperationStatus(), outcome.reasonCode())) {
			return true;
		}

		long sequenceNumber = nextAuditSequence(locked.paymentId());
		insertAuditEvent(
				locked.paymentId(),
				locked.operationId(),
				"PAYMENT_OPERATION",
				CONTRADICTION_EVENT_TYPE,
				locked.operationStatus(),
				mapping.newOperationStatus(),
				outcome.reasonCode(),
				correlationId,
				causationId,
				sequenceNumber,
				now);
		return true;
	}

	private boolean isDuplicateContradictionEvidence(UUID operationId, String newStatus, String reasonCode) {
		return jdbcClient.sql("""
				SELECT event_type, new_status, reason_code
				FROM payment_audit_events
				WHERE payment_operation_id = :operationId
				ORDER BY sequence_number DESC
				LIMIT 1
				""")
				.param("operationId", operationId)
				.query((resultSet, rowNumber) -> new LatestAuditEvent(
						resultSet.getString("event_type"),
						resultSet.getString("new_status"),
						resultSet.getString("reason_code")))
				.optional()
				.filter(latest -> CONTRADICTION_EVENT_TYPE.equals(latest.eventType()))
				.filter(latest -> Objects.equals(latest.newStatus(), newStatus))
				.filter(latest -> Objects.equals(latest.reasonCode(), reasonCode))
				.isPresent();
	}

	private boolean isContradiction(Locked locked, PspAuthorizationOutcome outcome, OutcomeMapping mapping) {
		if (!mapping.newOperationStatus().equals(locked.operationStatus())) {
			return true;
		}
		if ("CAPTURE".equals(locked.operationType()) && "REJECTED".equals(mapping.newOperationStatus())) {
			boolean recordedExpired = AUTHORIZATION_EXPIRED_REASON.equals(locked.pspResultCode());
			boolean newExpired = AUTHORIZATION_EXPIRED_REASON.equals(outcome.reasonCode());
			return recordedExpired != newExpired;
		}
		return false;
	}

	@Transactional
	boolean commitUnknownWithReconciliationJob(
			UUID operationId, UUID self, String reasonCode, UUID correlationId, UUID causationId, Instant now) {
		Optional<Locked> lockedOptional = lockOperation(operationId);
		if (lockedOptional.isEmpty()) {
			return false;
		}

		Locked locked = lockedOptional.get();
		if (isFinalizedStatus(locked.operationStatus())) {
			return true;
		}

		boolean alreadyUnknown = locked.operationStatus().equals("UNKNOWN");

		int rowsUpdated = jdbcClient.sql("""
				UPDATE payment_operations
				SET status = 'UNKNOWN',
				    psp_result_code = :pspResultCode,
				    unknown_since = COALESCE(unknown_since, :now),
				    lease_owner = NULL,
				    lease_until = NULL,
				    updated_at = :now
				WHERE id = :operationId
				  AND (lease_owner = :self OR lease_owner IS NULL)
				""")
				.param("pspResultCode", reasonCode)
				.param("now", databaseTimestamp(now))
				.param("operationId", operationId)
				.param("self", self)
				.update();

		if (rowsUpdated == 0) {
			return false;
		}

		if (!alreadyUnknown) {
			long sequenceNumber = nextAuditSequence(locked.paymentId());
			insertAuditEvent(
					locked.paymentId(),
					operationId,
					"PAYMENT_OPERATION",
					"PAYMENT_OPERATION_UNKNOWN",
					locked.operationStatus(),
					"UNKNOWN",
					reasonCode,
					correlationId,
					causationId,
					sequenceNumber,
					now);
		}

		jdbcClient.sql("""
				INSERT INTO reconciliation_jobs
				    (id, operation_id, unknown_since, next_check_at, attempts, created_at, updated_at)
				VALUES
				    (:id, :operationId, :now, :now, 0, :now, :now)
				ON CONFLICT (operation_id) DO NOTHING
				""")
				.param("id", UUID.randomUUID())
				.param("operationId", operationId)
				.param("now", databaseTimestamp(now))
				.update();

		return true;
	}

	private Optional<Locked> lockOperation(UUID operationId) {
		return jdbcClient.sql("""
				SELECT po.id AS operation_id,
				       po.type AS operation_type,
				       po.status AS operation_status,
				       po.psp_result_code AS psp_result_code,
				       p.id AS payment_id,
				       p.status AS payment_status
				FROM payment_operations po
				JOIN payments p ON p.id = po.payment_id
				WHERE po.id = :operationId
				FOR UPDATE
				""")
				.param("operationId", operationId)
				.query((resultSet, rowNumber) -> new Locked(
						resultSet.getObject("operation_id", UUID.class),
						resultSet.getString("operation_type"),
						resultSet.getString("operation_status"),
						resultSet.getString("psp_result_code"),
						resultSet.getObject("payment_id", UUID.class),
						resultSet.getString("payment_status")))
				.optional();
	}

	private Optional<OutcomeMapping> applyOutcome(
			Locked locked, PspAuthorizationOutcome outcome, UUID correlationId, UUID causationId, Instant now, UUID self) {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(locked.operationType(), outcome, now);

		int rowsUpdated = jdbcClient.sql("""
				UPDATE payment_operations
				SET status = :status,
				    psp_reference = :pspReference,
				    psp_result_code = :pspResultCode,
				    error_code = :errorCode,
				    completed_at = :completedAt,
				    unknown_since = :unknownSince,
				    lease_owner = NULL,
				    lease_until = NULL,
				    updated_at = :now
				WHERE id = :operationId
				  AND (lease_owner = :self OR lease_owner IS NULL)
				""")
				.param("status", mapping.newOperationStatus())
				.param("pspReference", outcome.pspReference())
				.param("pspResultCode", outcome.reasonCode())
				.param("errorCode", mapping.errorCode())
				.param("completedAt", mapping.completedAt() == null ? null : databaseTimestamp(mapping.completedAt()))
				.param("unknownSince", mapping.unknownSince() == null ? null : databaseTimestamp(mapping.unknownSince()))
				.param("now", databaseTimestamp(now))
				.param("operationId", locked.operationId())
				.param("self", self)
				.update();

		if (rowsUpdated == 0) {
			return Optional.empty();
		}

		long sequenceNumber = nextAuditSequence(locked.paymentId());
		insertAuditEvent(
				locked.paymentId(),
				locked.operationId(),
				"PAYMENT_OPERATION",
				"PAYMENT_OPERATION_" + mapping.newOperationStatus(),
				locked.operationStatus(),
				mapping.newOperationStatus(),
				outcome.reasonCode(),
				correlationId,
				causationId,
				sequenceNumber,
				now);

		if (mapping.newPaymentStatus() != null && isEligiblePaymentTransitionSource(locked)) {
			int paymentRowsUpdated = jdbcClient.sql("""
					UPDATE payments
					SET status = :status,
					    version = version + 1,
					    updated_at = :now
					WHERE id = :paymentId
					  AND status = :lockedPaymentStatus
					""")
					.param("status", mapping.newPaymentStatus())
					.param("now", databaseTimestamp(now))
					.param("paymentId", locked.paymentId())
					.param("lockedPaymentStatus", locked.paymentStatus())
					.update();

			if (paymentRowsUpdated == 1) {
				insertAuditEvent(
						locked.paymentId(),
						locked.operationId(),
						"PAYMENT",
						"PAYMENT_" + mapping.newPaymentStatus(),
						locked.paymentStatus(),
						mapping.newPaymentStatus(),
						null,
						correlationId,
						causationId,
						sequenceNumber + 1,
						now);
			}
		}

		return Optional.of(mapping);
	}

	private boolean isEligiblePaymentTransitionSource(Locked locked) {
		if (locked.operationType().equals("AUTHORIZE")) {
			return locked.paymentStatus().equals("CREATED");
		}
		return locked.paymentStatus().equals("AUTHORIZED");
	}

	private long nextAuditSequence(UUID paymentId) {
		return jdbcClient.sql("""
				SELECT COALESCE(MAX(sequence_number), 0) + 1
				FROM payment_audit_events
				WHERE payment_id = :paymentId
				""")
				.param("paymentId", paymentId)
				.query(Long.class)
				.single();
	}

	private void insertAuditEvent(
			UUID paymentId,
			UUID operationId,
			String entityType,
			String eventType,
			String previousStatus,
			String newStatus,
			String reasonCode,
			UUID correlationId,
			UUID causationId,
			long sequenceNumber,
			Instant occurredAt) {
		jdbcClient.sql("""
				INSERT INTO payment_audit_events
				    (event_id, payment_id, payment_operation_id, entity_type, event_type, previous_status, new_status,
				     reason_code, source, correlation_id, causation_id, sequence_number, metadata, occurred_at, recorded_at)
				VALUES
				    (:eventId, :paymentId, :operationId, :entityType, :eventType, :previousStatus, :newStatus,
				     :reasonCode, 'PSP_PROCESSOR', :correlationId, :causationId, :sequenceNumber, '{}'::jsonb, :occurredAt, :occurredAt)
				""")
				.param("eventId", UUID.randomUUID())
				.param("paymentId", paymentId)
				.param("operationId", operationId)
				.param("entityType", entityType)
				.param("eventType", eventType)
				.param("previousStatus", previousStatus)
				.param("newStatus", newStatus)
				.param("reasonCode", reasonCode)
				.param("correlationId", correlationId)
				.param("causationId", causationId)
				.param("sequenceNumber", sequenceNumber)
				.param("occurredAt", databaseTimestamp(occurredAt))
				.update();
	}

	private boolean isFinalizedStatus(String status) {
		return status.equals("SUCCEEDED") || status.equals("REJECTED") || status.equals("FAILED");
	}

	private PaymentOperationContext toContext(ResultSet resultSet) throws SQLException {
		return new PaymentOperationContext(
				resultSet.getObject("operation_id", UUID.class),
				resultSet.getString("operation_type"),
				resultSet.getString("operation_status"),
				resultSet.getLong("amount_minor"),
				resultSet.getString("currency"),
				resultSet.getObject("payment_id", UUID.class),
				resultSet.getObject("merchant_id", UUID.class),
				resultSet.getString("payment_status"),
				resultSet.getInt("attempt_count"),
				resultSet.getObject("lease_owner", UUID.class),
				resultSet.getBoolean("has_unresolved_job"),
				resultSet.getString("authorization_psp_reference"));
	}

	private OffsetDateTime databaseTimestamp(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

	private record Locked(
			UUID operationId, String operationType, String operationStatus, String pspResultCode, UUID paymentId, String paymentStatus) {
	}

	enum DefinitiveResolution {
		APPLIED, DUPLICATE, CONTRADICTION, LEASE_LOST
	}

	private record LatestAuditEvent(String eventType, String newStatus, String reasonCode) {
	}

	private record LeaseRow(String status, UUID leaseOwner, OffsetDateTime leaseUntil, int attemptCount) {
	}

	record LeaseClaim(Status status, int attemptCount, UUID previousOwner) {

		enum Status {
			NOT_FOUND, FINALIZED, DENIED, ACQUIRED
		}

		boolean isTakeover(UUID self) {
			return status == Status.ACQUIRED
					&& previousOwner != null
					&& !previousOwner.equals(self)
					&& attemptCount > 0;
		}
	}
}
