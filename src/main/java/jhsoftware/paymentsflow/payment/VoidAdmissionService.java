package jhsoftware.paymentsflow.payment;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
class VoidAdmissionService {

	private static final String ADMISSIONS_METRIC = "paymentflow.api.admissions";

	private final VoidAdmissionRepository repository;
	private final PaymentProperties properties;
	private final VoidRequestHasher requestHasher;
	private final Clock clock;
	private final MeterRegistry meterRegistry;

	VoidAdmissionService(
			VoidAdmissionRepository repository,
			PaymentProperties properties,
			VoidRequestHasher requestHasher,
			Clock clock,
			MeterRegistry meterRegistry) {
		this.repository = repository;
		this.properties = properties;
		this.requestHasher = requestHasher;
		this.clock = clock;
		this.meterRegistry = meterRegistry;
	}

	CreatePaymentResponse admit(String rawPaymentId, String idempotencyKey) {
		try {
			UUID paymentId = parsePaymentId(rawPaymentId);

			VoidAdmissionRepository.VoidDerivation derivation = repository
					.findVoidDerivation(properties.merchantId(), paymentId)
					.orElseThrow(PaymentNotFoundException::new);

			VoidAdmissionCommand command = new VoidAdmissionCommand(
					paymentId, derivation.amountMinor(), derivation.currency(), idempotencyKey);
			String requestHash = requestHasher.hash(command);

			Optional<CreatePaymentResponse> precheckedAdmission =
					repository.precheckAdmission(properties.merchantId(), command, requestHash);
			if (precheckedAdmission.isPresent()) {
				recordAdmission("DUPLICATE");
				return precheckedAdmission.get();
			}

			CreatePaymentResponse response =
					repository.admitVoid(properties.merchantId(), command, requestHash, clock.instant());
			recordAdmission("ADMITTED");
			return response;
		}
		catch (RuntimeException exception) {
			recordAdmission("REJECTED");
			throw exception;
		}
	}

	private void recordAdmission(String outcome) {
		Counter.builder(ADMISSIONS_METRIC)
				.tag("operation_type", "VOID")
				.tag("outcome", outcome)
				.register(meterRegistry)
				.increment();
	}

	private UUID parsePaymentId(String rawPaymentId) {
		try {
			return UUID.fromString(rawPaymentId);
		}
		catch (IllegalArgumentException exception) {
			throw new InvalidPaymentIdException();
		}
	}
}
