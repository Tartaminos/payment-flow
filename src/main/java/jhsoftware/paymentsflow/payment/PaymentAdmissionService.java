package jhsoftware.paymentsflow.payment;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Service
class PaymentAdmissionService {

	private static final String ADMISSIONS_METRIC = "paymentflow.api.admissions";

	private final PaymentAdmissionRepository repository;
	private final PaymentProperties properties;
	private final PaymentRequestHasher requestHasher;
	private final Clock clock;
	private final MeterRegistry meterRegistry;

	PaymentAdmissionService(
			PaymentAdmissionRepository repository,
			PaymentProperties properties,
			PaymentRequestHasher requestHasher,
			Clock clock,
			MeterRegistry meterRegistry) {
		this.repository = repository;
		this.properties = properties;
		this.requestHasher = requestHasher;
		this.clock = clock;
		this.meterRegistry = meterRegistry;
	}

	@Transactional
	CreatePaymentResponse admit(PaymentAdmissionCommand command) {
		try {
			String requestHash = requestHasher.hash(command);
			repository.lockIdempotencyScope(properties.merchantId(), command.idempotencyKey());

			var existingAdmission = repository.findExistingAdmission(
					properties.merchantId(), command.idempotencyKey());
			if (existingAdmission.isPresent()) {
				if (!existingAdmission.get().requestHash().equals(requestHash)) {
					recordAdmission("REJECTED");
					throw new IdempotencyKeyConflictException();
				}
				recordAdmission("DUPLICATE");
				return existingAdmission.get().response();
			}

			UUID paymentId = UUID.randomUUID();
			UUID operationId = UUID.randomUUID();
			UUID idempotencyId = UUID.randomUUID();
			UUID eventId = UUID.randomUUID();
			UUID messageId = UUID.randomUUID();
			UUID correlationId = UUID.randomUUID();
			Instant admittedAt = clock.instant();

			CorrelationContext.put(CorrelationContext.CORRELATION_ID, correlationId);
			CorrelationContext.put(CorrelationContext.PAYMENT_ID, paymentId);
			CorrelationContext.put(CorrelationContext.OPERATION_ID, operationId);

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

			recordAdmission("ADMITTED");
			return new CreatePaymentResponse(paymentId, operationId, "CREATED", "PENDING");
		}
		finally {
			CorrelationContext.clear();
		}
	}

	private void recordAdmission(String outcome) {
		Counter.builder(ADMISSIONS_METRIC)
				.tag("operation_type", "CREATE")
				.tag("outcome", outcome)
				.register(meterRegistry)
				.increment();
	}
}
