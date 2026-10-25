package jhsoftware.paymentsflow.processor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationProcessingRepository {

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
				       p.id AS payment_id,
				       p.merchant_id AS merchant_id,
				       p.currency AS currency,
				       p.status AS payment_status
				FROM payment_operations po
				JOIN payments p ON p.id = po.payment_id
				WHERE po.id = :operationId
				""")
				.param("operationId", operationId)
				.query((resultSet, rowNumber) -> toContext(resultSet))
				.optional();
	}

	@Transactional
	boolean commitOutcome(UUID operationId, PspAuthorizationOutcome outcome, UUID correlationId, UUID causationId, Instant now) {
		Optional<Locked> lockedOptional = jdbcClient.sql("""
				SELECT po.id AS operation_id,
				       po.status AS operation_status,
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
						resultSet.getString("operation_status"),
						resultSet.getObject("payment_id", UUID.class),
						resultSet.getString("payment_status")))
				.optional();

		if (lockedOptional.isEmpty()) {
			return false;
		}

		Locked locked = lockedOptional.get();
		if (isFinalizedStatus(locked.operationStatus())) {
			return true;
		}

		OutcomeMapping mapping = OutcomeMapping.forOutcome(outcome, now);

		jdbcClient.sql("""
				UPDATE payment_operations
				SET status = :status,
				    psp_reference = :pspReference,
				    psp_result_code = :pspResultCode,
				    error_code = :errorCode,
				    completed_at = :completedAt,
				    unknown_since = :unknownSince,
				    updated_at = :now
				WHERE id = :operationId
				""")
				.param("status", mapping.newOperationStatus())
				.param("pspReference", outcome.pspReference())
				.param("pspResultCode", outcome.reasonCode())
				.param("errorCode", mapping.errorCode())
				.param("completedAt", mapping.completedAt() == null ? null : databaseTimestamp(mapping.completedAt()))
				.param("unknownSince", mapping.unknownSince() == null ? null : databaseTimestamp(mapping.unknownSince()))
				.param("now", databaseTimestamp(now))
				.param("operationId", operationId)
				.update();

		long sequenceNumber = nextAuditSequence(locked.paymentId());
		insertAuditEvent(
				locked.paymentId(),
				operationId,
				"PAYMENT_OPERATION",
				"PAYMENT_OPERATION_" + mapping.newOperationStatus(),
				locked.operationStatus(),
				mapping.newOperationStatus(),
				outcome.reasonCode(),
				correlationId,
				causationId,
				sequenceNumber,
				now);

		if (mapping.newPaymentStatus() != null) {
			jdbcClient.sql("""
					UPDATE payments
					SET status = :status,
					    version = version + 1,
					    updated_at = :now
					WHERE id = :paymentId
					""")
					.param("status", mapping.newPaymentStatus())
					.param("now", databaseTimestamp(now))
					.param("paymentId", locked.paymentId())
					.update();

			insertAuditEvent(
					locked.paymentId(),
					operationId,
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

		return mapping.ackEligible();
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
				resultSet.getString("payment_status"));
	}

	private OffsetDateTime databaseTimestamp(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

	private record Locked(UUID operationId, String operationStatus, UUID paymentId, String paymentStatus) {
	}
}
