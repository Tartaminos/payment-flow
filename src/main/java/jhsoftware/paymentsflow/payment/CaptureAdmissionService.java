package jhsoftware.paymentsflow.payment;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
class CaptureAdmissionService {

	private static final String ADMISSIONS_METRIC = "paymentflow.api.admissions";

	private final CaptureAdmissionRepository repository;
	private final PaymentProperties properties;
	private final CaptureRequestHasher requestHasher;
	private final CaptureAuthorizationValidityClient validityClient;
	private final Clock clock;
	private final MeterRegistry meterRegistry;

	CaptureAdmissionService(
			CaptureAdmissionRepository repository,
			PaymentProperties properties,
			CaptureRequestHasher requestHasher,
			CaptureAuthorizationValidityClient validityClient,
			Clock clock,
			MeterRegistry meterRegistry) {
		this.repository = repository;
		this.properties = properties;
		this.requestHasher = requestHasher;
		this.validityClient = validityClient;
		this.clock = clock;
		this.meterRegistry = meterRegistry;
	}

	CreatePaymentResponse admit(String rawPaymentId, String idempotencyKey) {
		try {
			UUID paymentId = parsePaymentId(rawPaymentId);

			CaptureAdmissionRepository.CaptureDerivation derivation = repository
					.findCaptureDerivation(properties.merchantId(), paymentId)
					.orElseThrow(PaymentNotFoundException::new);

			CaptureAdmissionCommand command = new CaptureAdmissionCommand(
					paymentId, derivation.amountMinor(), derivation.currency(), idempotencyKey);
			String requestHash = requestHasher.hash(command);

			Optional<CreatePaymentResponse> precheckedAdmission =
					repository.precheckAdmission(properties.merchantId(), command, requestHash);
			if (precheckedAdmission.isPresent()) {
				recordAdmission("DUPLICATE");
				return precheckedAdmission.get();
			}

			UUID authorizationPspOperationId = parseAuthorizationPspOperationId(derivation.authorizationPspReference());
			AuthorizationValidityEvidence validityEvidence = validityClient.checkValidity(authorizationPspOperationId);
			if (validityEvidence.status() == AuthorizationValidityStatus.UNKNOWN) {
				throw new PspStatusUnavailableException();
			}

			CreatePaymentResponse response = repository.admitCapture(
					properties.merchantId(),
					command,
					requestHash,
					authorizationPspOperationId,
					validityEvidence,
					clock.instant());
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
				.tag("operation_type", "CAPTURE")
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

	private UUID parseAuthorizationPspOperationId(String authorizationPspReference) {
		if (authorizationPspReference == null) {
			throw new PspStatusUnavailableException();
		}
		try {
			return UUID.fromString(authorizationPspReference);
		}
		catch (IllegalArgumentException exception) {
			throw new PspStatusUnavailableException();
		}
	}
}
