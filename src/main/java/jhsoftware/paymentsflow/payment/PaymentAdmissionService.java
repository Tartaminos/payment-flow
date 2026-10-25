package jhsoftware.paymentsflow.payment;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class PaymentAdmissionService {

	private final PaymentAdmissionRepository repository;
	private final PaymentProperties properties;
	private final PaymentRequestHasher requestHasher;
	private final Clock clock;

	PaymentAdmissionService(
			PaymentAdmissionRepository repository,
			PaymentProperties properties,
			PaymentRequestHasher requestHasher,
			Clock clock) {
		this.repository = repository;
		this.properties = properties;
		this.requestHasher = requestHasher;
		this.clock = clock;
	}

	@Transactional
	CreatePaymentResponse admit(PaymentAdmissionCommand command) {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID idempotencyId = UUID.randomUUID();
		UUID eventId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		Instant admittedAt = clock.instant();
		String requestHash = requestHasher.hash(command);

		repository.insertPayment(paymentId, properties.merchantId(), command, admittedAt);
		repository.insertOperation(operationId, paymentId, command.amountMinor(), admittedAt);
		repository.insertIdempotencyKey(
				idempotencyId,
				properties.merchantId(),
				command.idempotencyKey(),
				requestHash,
				paymentId,
				operationId,
				admittedAt);
		repository.insertAuditEvent(eventId, paymentId, operationId, correlationId, admittedAt);
		repository.insertOutboxEvent(messageId, paymentId, operationId, correlationId, admittedAt);

		return new CreatePaymentResponse(paymentId, operationId, "CREATED", "PENDING");
	}
}
