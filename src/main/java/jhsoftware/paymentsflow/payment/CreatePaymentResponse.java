package jhsoftware.paymentsflow.payment;

import java.util.UUID;

public record CreatePaymentResponse(
		UUID paymentId,
		UUID operationId,
		String status,
		String operationStatus) {
}
