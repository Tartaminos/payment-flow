package jhsoftware.paymentsflow.payment;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;

@Service
class VoidAdmissionService {

	private final VoidAdmissionRepository repository;
	private final PaymentProperties properties;
	private final VoidRequestHasher requestHasher;
	private final Clock clock;

	VoidAdmissionService(
			VoidAdmissionRepository repository,
			PaymentProperties properties,
			VoidRequestHasher requestHasher,
			Clock clock) {
		this.repository = repository;
		this.properties = properties;
		this.requestHasher = requestHasher;
		this.clock = clock;
	}

	CreatePaymentResponse admit(String rawPaymentId, String idempotencyKey) {
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
			return precheckedAdmission.get();
		}

		return repository.admitVoid(properties.merchantId(), command, requestHash, clock.instant());
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
