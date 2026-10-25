package jhsoftware.paymentsflow.payment;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class PaymentAdmissionRepository {

	private final JdbcClient jdbcClient;

	PaymentAdmissionRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	void lockIdempotencyScope(UUID merchantId, String idempotencyKey) {
		jdbcClient.sql("""
				SELECT 1
				FROM pg_advisory_xact_lock(hashtextextended(
				    CAST(:merchantId AS text) || ':' || 'PAYMENT_CREATE' || ':' || :idempotencyKey,
				    0))
				""")
				.param("merchantId", merchantId)
				.param("idempotencyKey", idempotencyKey)
				.query(Integer.class)
				.single();
	}

	Optional<ExistingAdmission> findExistingAdmission(UUID merchantId, String idempotencyKey) {
		return jdbcClient.sql("""
				SELECT ik.request_hash,
				       ik.payment_id,
				       ik.payment_operation_id,
				       p.status AS payment_status,
				       po.status AS operation_status
				FROM idempotency_keys ik
				JOIN payments p ON p.id = ik.payment_id
				JOIN payment_operations po ON po.id = ik.payment_operation_id
				WHERE ik.merchant_id = :merchantId
				  AND ik.operation_scope = 'PAYMENT_CREATE'
				  AND ik.key = :idempotencyKey
				""")
				.param("merchantId", merchantId)
				.param("idempotencyKey", idempotencyKey)
				.query((resultSet, rowNumber) -> new ExistingAdmission(
						resultSet.getString("request_hash"),
						new CreatePaymentResponse(
								resultSet.getObject("payment_id", UUID.class),
								resultSet.getObject("payment_operation_id", UUID.class),
								resultSet.getString("payment_status"),
								resultSet.getString("operation_status"))))
				.optional();
	}

	void insertPayment(UUID paymentId, UUID merchantId, PaymentAdmissionCommand command, Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (:id, :merchantId, :orderId, :amountMinor, :currency, 'CREATED', 0, :admittedAt, :admittedAt)
				""")
				.param("id", paymentId)
				.param("merchantId", merchantId)
				.param("orderId", command.orderId())
				.param("amountMinor", command.amountMinor())
				.param("currency", command.currency())
				.param("admittedAt", databaseTimestamp(admittedAt))
				.update();
	}

	void insertOperation(UUID operationId, UUID paymentId, long amountMinor, Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, 'AUTHORIZE', 'PENDING', :amountMinor, :pspIdempotencyKey, 0, :admittedAt, :admittedAt)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("amountMinor", amountMinor)
				.param("pspIdempotencyKey", operationId.toString())
				.param("admittedAt", databaseTimestamp(admittedAt))
				.update();
	}

	void insertIdempotencyKey(
			UUID id,
			UUID merchantId,
			String idempotencyKey,
			String requestHash,
			UUID paymentId,
			UUID operationId,
			Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO idempotency_keys
				    (id, merchant_id, operation_scope, key, request_hash, payment_id, payment_operation_id, created_at, expires_at)
				VALUES
				    (:id, :merchantId, 'PAYMENT_CREATE', :idempotencyKey, :requestHash, :paymentId, :operationId, :admittedAt, NULL)
				""")
				.params(Map.of(
						"id", id,
						"merchantId", merchantId,
						"idempotencyKey", idempotencyKey,
						"requestHash", requestHash,
						"paymentId", paymentId,
						"operationId", operationId,
						"admittedAt", databaseTimestamp(admittedAt)))
				.update();
	}

	void insertAuditEvent(
			UUID eventId,
			UUID paymentId,
			UUID operationId,
			UUID correlationId,
			Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO payment_audit_events
				    (event_id, payment_id, payment_operation_id, entity_type, event_type, previous_status, new_status,
				     reason_code, source, correlation_id, causation_id, sequence_number, metadata, occurred_at, recorded_at)
				VALUES
				    (:eventId, :paymentId, :operationId, 'PAYMENT', 'PAYMENT_CREATED', NULL, 'CREATED',
				     NULL, 'API', :correlationId, NULL, 1, '{}'::jsonb, :admittedAt, :admittedAt)
				""")
				.params(Map.of(
						"eventId", eventId,
						"paymentId", paymentId,
						"operationId", operationId,
						"correlationId", correlationId,
						"admittedAt", databaseTimestamp(admittedAt)))
				.update();
	}

	void insertOutboxEvent(
			UUID messageId,
			UUID paymentId,
			UUID operationId,
			UUID correlationId,
			Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO outbox_events
				    (message_id, payment_id, operation_id, command_type, correlation_id, causation_id,
				     occurred_at, status, created_at, published_at)
				VALUES
				    (:messageId, :paymentId, :operationId, 'ExecutePaymentOperation', :correlationId, NULL,
				     :admittedAt, 'PENDING', :admittedAt, NULL)
				""")
				.params(Map.of(
						"messageId", messageId,
						"paymentId", paymentId,
						"operationId", operationId,
						"correlationId", correlationId,
						"admittedAt", databaseTimestamp(admittedAt)))
				.update();
	}

	private OffsetDateTime databaseTimestamp(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

	record ExistingAdmission(String requestHash, CreatePaymentResponse response) {
	}
}
