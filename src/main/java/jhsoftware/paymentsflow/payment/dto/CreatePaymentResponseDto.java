package jhsoftware.paymentsflow.payment.dto;

import java.util.UUID;

public record CreatePaymentResponseDto(
		UUID paymentId,
		UUID operationId,
		String status,
		String operationStatus) {
}
