package jhsoftware.paymentsflow.payment;

import java.util.UUID;

import org.springframework.stereotype.Service;

@Service
class PaymentStatusService {

	private final PaymentStatusRepository repository;
	private final PaymentProperties properties;

	PaymentStatusService(PaymentStatusRepository repository, PaymentProperties properties) {
		this.repository = repository;
		this.properties = properties;
	}

	PaymentStatusResponse getStatus(String rawPaymentId) {
		UUID paymentId = parsePaymentId(rawPaymentId);
		return repository.findStatus(properties.merchantId(), paymentId)
				.orElseThrow(PaymentNotFoundException::new);
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
