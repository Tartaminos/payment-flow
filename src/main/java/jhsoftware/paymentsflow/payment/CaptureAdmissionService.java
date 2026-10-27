package jhsoftware.paymentsflow.payment;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;

@Service
class CaptureAdmissionService {

	private final CaptureAdmissionRepository repository;
	private final PaymentProperties properties;
	private final CaptureRequestHasher requestHasher;
	private final CaptureAuthorizationValidityClient validityClient;
	private final Clock clock;

	CaptureAdmissionService(
			CaptureAdmissionRepository repository,
			PaymentProperties properties,
			CaptureRequestHasher requestHasher,
			CaptureAuthorizationValidityClient validityClient,
			Clock clock) {
		this.repository = repository;
		this.properties = properties;
		this.requestHasher = requestHasher;
		this.validityClient = validityClient;
		this.clock = clock;
	}

	CreatePaymentResponse admit(String rawPaymentId, String idempotencyKey) {
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
			return precheckedAdmission.get();
		}

		UUID authorizationPspOperationId = parseAuthorizationPspOperationId(derivation.authorizationPspReference());
		AuthorizationValidityEvidence validityEvidence = validityClient.checkValidity(authorizationPspOperationId);
		if (validityEvidence.status() == AuthorizationValidityStatus.UNKNOWN) {
			throw new PspStatusUnavailableException();
		}

		return repository.admitCapture(
				properties.merchantId(),
				command,
				requestHash,
				authorizationPspOperationId,
				validityEvidence,
				clock.instant());
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
