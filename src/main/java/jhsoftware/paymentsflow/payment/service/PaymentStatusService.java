package jhsoftware.paymentsflow.payment.service;

import java.util.UUID;

import org.springframework.stereotype.Service;

import jhsoftware.paymentsflow.payment.config.PaymentProperties;
import jhsoftware.paymentsflow.payment.dto.PaymentStatusResponseDto;
import jhsoftware.paymentsflow.payment.exception.InvalidPaymentIdException;
import jhsoftware.paymentsflow.payment.exception.PaymentNotFoundException;
import jhsoftware.paymentsflow.payment.repository.PaymentStatusRepository;

@Service
public class PaymentStatusService {

	private final PaymentStatusRepository repository;
	private final PaymentProperties properties;

	PaymentStatusService(PaymentStatusRepository repository, PaymentProperties properties) {
		this.repository = repository;
		this.properties = properties;
	}

	public PaymentStatusResponseDto getStatus(String rawPaymentId) {
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
