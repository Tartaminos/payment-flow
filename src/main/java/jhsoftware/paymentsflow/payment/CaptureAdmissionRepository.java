package jhsoftware.paymentsflow.payment;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Repository
class CaptureAdmissionRepository {

	private static final ObjectMapper METADATA_MAPPER = new ObjectMapper();

	private final JdbcClient jdbcClient;

	CaptureAdmissionRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	Optional<CaptureDerivation> findCaptureDerivation(UUID merchantId, UUID paymentId) {
		return jdbcClient.sql("""
				SELECT p.amount_minor AS amount_minor,
				       p.currency AS currency,
				       po.psp_reference AS authorization_psp_reference
				FROM payments p
				LEFT JOIN payment_operations po
				    ON po.payment_id = p.id AND po.type = 'AUTHORIZE'
				WHERE p.id = :paymentId
				  AND p.merchant_id = :merchantId
				""")
				.param("paymentId", paymentId)
				.param("merchantId", merchantId)
				.query((resultSet, rowNumber) -> new CaptureDerivation(
						resultSet.getLong("amount_minor"),
						resultSet.getString("currency"),
						resultSet.getString("authorization_psp_reference")))
				.optional();
	}

	@Transactional(readOnly = true)
	Optional<CreatePaymentResponse> precheckAdmission(UUID merchantId, CaptureAdmissionCommand command, String requestHash) {
		Optional<ExistingAdmission> existingAdmission = findExistingAdmission(merchantId, command.idempotencyKey());
		if (existingAdmission.isPresent()) {
			if (!existingAdmission.get().requestHash().equals(requestHash)) {
				throw new IdempotencyKeyConflictException();
			}
			return Optional.of(existingAdmission.get().response());
		}

		String paymentStatus = findPaymentStatus(merchantId, command.paymentId())
				.orElseThrow(PaymentNotFoundException::new);

		if (paymentStatus.equals("CAPTURED")) {
			throw new PaymentAlreadyCapturedException();
		}
		if (paymentStatus.equals("EXPIRED")) {
			throw new AuthorizationExpiredException();
		}
		if (!paymentStatus.equals("AUTHORIZED")) {
			throw new PaymentNotEligibleException();
		}
		if (hasUnresolvedConflictingOperation(command.paymentId())) {
			throw new PaymentOperationInProgressException();
		}

		return Optional.empty();
	}

	@Transactional(noRollbackFor = AuthorizationExpiredException.class)
	CreatePaymentResponse admitCapture(
			UUID merchantId,
			CaptureAdmissionCommand command,
			String requestHash,
			UUID authorizationPspOperationId,
			AuthorizationValidityEvidence validityEvidence,
			Instant admittedAt) {
		lockIdempotencyScope(merchantId, command.idempotencyKey());

		Optional<ExistingAdmission> existingAdmission = findExistingAdmission(merchantId, command.idempotencyKey());
		if (existingAdmission.isPresent()) {
			if (!existingAdmission.get().requestHash().equals(requestHash)) {
				throw new IdempotencyKeyConflictException();
			}
			return existingAdmission.get().response();
		}

		String paymentStatus = lockPaymentStatus(merchantId, command.paymentId())
				.orElseThrow(PaymentNotFoundException::new);

		if (paymentStatus.equals("CAPTURED")) {
			throw new PaymentAlreadyCapturedException();
		}
		if (paymentStatus.equals("EXPIRED")) {
			throw new AuthorizationExpiredException();
		}
		if (!paymentStatus.equals("AUTHORIZED")) {
			throw new PaymentNotEligibleException();
		}

		if (hasUnresolvedConflictingOperation(command.paymentId())) {
			throw new PaymentOperationInProgressException();
		}

		if (validityEvidence.status() == AuthorizationValidityStatus.EXPIRED) {
			expirePayment(merchantId, command.paymentId(), authorizationPspOperationId, validityEvidence, admittedAt);
			throw new AuthorizationExpiredException();
		}

		UUID operationId = UUID.randomUUID();
		UUID idempotencyId = UUID.randomUUID();
		UUID eventId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();

		try {
			CorrelationContext.put(CorrelationContext.CORRELATION_ID, correlationId);
			CorrelationContext.put(CorrelationContext.PAYMENT_ID, command.paymentId());
			CorrelationContext.put(CorrelationContext.OPERATION_ID, operationId);

			insertOperation(operationId, command.paymentId(), command.amountMinor(), admittedAt);
			insertIdempotencyKey(
					idempotencyId,
					merchantId,
					command.idempotencyKey(),
					requestHash,
					command.paymentId(),
					operationId,
					admittedAt);
			insertAuditEvent(
					eventId,
					command.paymentId(),
					operationId,
					correlationId,
					nextAuditSequence(command.paymentId()),
					admittedAt);
			insertOutboxEvent(messageId, command.paymentId(), operationId, correlationId, admittedAt);

			return new CreatePaymentResponse(command.paymentId(), operationId, paymentStatus, "PENDING");
		}
		finally {
			CorrelationContext.clear();
		}
	}

	private void expirePayment(
			UUID merchantId,
			UUID paymentId,
			UUID authorizationPspOperationId,
			AuthorizationValidityEvidence validityEvidence,
			Instant now) {
		int rowsUpdated = jdbcClient.sql("""
				UPDATE payments
				SET status = 'EXPIRED',
				    version = version + 1,
				    updated_at = :now
				WHERE id = :paymentId
				  AND merchant_id = :merchantId
				  AND status = 'AUTHORIZED'
				""")
				.param("now", databaseTimestamp(now))
				.param("paymentId", paymentId)
				.param("merchantId", merchantId)
				.update();

		if (rowsUpdated == 0) {
			return;
		}

		insertExpiryAuditEvent(
				UUID.randomUUID(),
				paymentId,
				UUID.randomUUID(),
				nextAuditSequence(paymentId),
				expiryMetadata(authorizationPspOperationId, validityEvidence),
				now);
	}

	private String expiryMetadata(UUID authorizationPspOperationId, AuthorizationValidityEvidence validityEvidence) {
		try {
			String validityEvidenceAt = validityEvidence.validityEvidenceAt() == null
					? null
					: validityEvidence.validityEvidenceAt().toString();
			return METADATA_MAPPER.writeValueAsString(
					new ExpiryEvidenceMetadata(authorizationPspOperationId.toString(), validityEvidenceAt));
		}
		catch (JsonProcessingException exception) {
			throw new IllegalStateException(exception);
		}
	}

	private void lockIdempotencyScope(UUID merchantId, String idempotencyKey) {
		jdbcClient.sql("""
				SELECT 1
				FROM pg_advisory_xact_lock(hashtextextended(
				    CAST(:merchantId AS text) || ':' || 'PAYMENT_CAPTURE' || ':' || :idempotencyKey,
				    0))
				""")
				.param("merchantId", merchantId)
				.param("idempotencyKey", idempotencyKey)
				.query(Integer.class)
				.single();
	}

	private Optional<ExistingAdmission> findExistingAdmission(UUID merchantId, String idempotencyKey) {
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
				  AND ik.operation_scope = 'PAYMENT_CAPTURE'
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

	private Optional<String> findPaymentStatus(UUID merchantId, UUID paymentId) {
		return jdbcClient.sql("""
				SELECT status
				FROM payments
				WHERE id = :paymentId
				  AND merchant_id = :merchantId
				""")
				.param("paymentId", paymentId)
				.param("merchantId", merchantId)
				.query(String.class)
				.optional();
	}

	private Optional<String> lockPaymentStatus(UUID merchantId, UUID paymentId) {
		return jdbcClient.sql("""
				SELECT status
				FROM payments
				WHERE id = :paymentId
				  AND merchant_id = :merchantId
				FOR UPDATE
				""")
				.param("paymentId", paymentId)
				.param("merchantId", merchantId)
				.query(String.class)
				.optional();
	}

	private boolean hasUnresolvedConflictingOperation(UUID paymentId) {
		return jdbcClient.sql("""
				SELECT EXISTS (
				    SELECT 1
				    FROM payment_operations
				    WHERE payment_id = :paymentId
				      AND type IN ('CAPTURE', 'VOID')
				      AND status IN ('PENDING', 'PROCESSING', 'UNKNOWN')
				)
				""")
				.param("paymentId", paymentId)
				.query(Boolean.class)
				.single();
	}

	private void insertOperation(UUID operationId, UUID paymentId, long amountMinor, Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, 'CAPTURE', 'PENDING', :amountMinor, :pspIdempotencyKey, 0, :admittedAt, :admittedAt)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("amountMinor", amountMinor)
				.param("pspIdempotencyKey", operationId.toString())
				.param("admittedAt", databaseTimestamp(admittedAt))
				.update();
	}

	private void insertIdempotencyKey(
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
				    (:id, :merchantId, 'PAYMENT_CAPTURE', :idempotencyKey, :requestHash, :paymentId, :operationId, :admittedAt, NULL)
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
			UUID eventId,
			UUID paymentId,
			UUID operationId,
			UUID correlationId,
			long sequenceNumber,
			Instant admittedAt) {
		jdbcClient.sql("""
				INSERT INTO payment_audit_events
				    (event_id, payment_id, payment_operation_id, entity_type, event_type, previous_status, new_status,
				     reason_code, source, correlation_id, causation_id, sequence_number, metadata, occurred_at, recorded_at)
				VALUES
				    (:eventId, :paymentId, :operationId, 'PAYMENT_OPERATION', 'PAYMENT_OPERATION_CREATED', NULL, 'PENDING',
				     NULL, 'API', :correlationId, NULL, :sequenceNumber, '{}'::jsonb, :admittedAt, :admittedAt)
				""")
				.params(Map.of(
						"eventId", eventId,
						"paymentId", paymentId,
						"operationId", operationId,
						"correlationId", correlationId,
						"sequenceNumber", sequenceNumber,
						"admittedAt", databaseTimestamp(admittedAt)))
				.update();
	}

	private void insertExpiryAuditEvent(
			UUID eventId,
			UUID paymentId,
			UUID correlationId,
			long sequenceNumber,
			String metadata,
			Instant occurredAt) {
		jdbcClient.sql("""
				INSERT INTO payment_audit_events
				    (event_id, payment_id, payment_operation_id, entity_type, event_type, previous_status, new_status,
				     reason_code, source, correlation_id, causation_id, sequence_number, metadata, occurred_at, recorded_at)
				VALUES
				    (:eventId, :paymentId, NULL, 'PAYMENT', 'PAYMENT_EXPIRED', 'AUTHORIZED', 'EXPIRED',
				     'PSP_AUTHORIZATION_EXPIRED', 'API', :correlationId, NULL, :sequenceNumber, CAST(:metadata AS jsonb), :occurredAt, :occurredAt)
				""")
				.param("eventId", eventId)
				.param("paymentId", paymentId)
				.param("correlationId", correlationId)
				.param("sequenceNumber", sequenceNumber)
				.param("metadata", metadata)
				.param("occurredAt", databaseTimestamp(occurredAt))
				.update();
	}

	private void insertOutboxEvent(
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

	record CaptureDerivation(long amountMinor, String currency, String authorizationPspReference) {
	}

	record ExistingAdmission(String requestHash, CreatePaymentResponse response) {
	}

	private record ExpiryEvidenceMetadata(String authorizationPspOperationId, String validityEvidenceAt) {
	}
}
