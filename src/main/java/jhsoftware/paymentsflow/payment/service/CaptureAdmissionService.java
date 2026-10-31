package jhsoftware.paymentsflow.payment.service;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import jhsoftware.paymentsflow.payment.client.CaptureAuthorizationValidityClient;
import jhsoftware.paymentsflow.payment.config.PaymentProperties;
import jhsoftware.paymentsflow.payment.command.CaptureAdmissionCommand;
import jhsoftware.paymentsflow.payment.dto.CreatePaymentResponseDto;
import jhsoftware.paymentsflow.payment.exception.InvalidPaymentIdException;
import jhsoftware.paymentsflow.payment.exception.PaymentNotFoundException;
import jhsoftware.paymentsflow.payment.exception.PspStatusUnavailableException;
import jhsoftware.paymentsflow.payment.model.AuthorizationValidityEvidence;
import jhsoftware.paymentsflow.payment.model.AuthorizationValidityStatus;
import jhsoftware.paymentsflow.payment.repository.CaptureAdmissionRepository;

@Service
public class CaptureAdmissionService {

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

	public CreatePaymentResponseDto admit(String rawPaymentId, String idempotencyKey) {
		try {
			UUID paymentId = parsePaymentId(rawPaymentId);

			CaptureAdmissionRepository.CaptureDerivation derivation = repository
					.findCaptureDerivation(properties.merchantId(), paymentId)
					.orElseThrow(PaymentNotFoundException::new);

			CaptureAdmissionCommand command = new CaptureAdmissionCommand(
					paymentId, derivation.amountMinor(), derivation.currency(), idempotencyKey);
			String requestHash = requestHasher.hash(command);

			Optional<CreatePaymentResponseDto> precheckedAdmission =
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

			CreatePaymentResponseDto response = repository.admitCapture(
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
