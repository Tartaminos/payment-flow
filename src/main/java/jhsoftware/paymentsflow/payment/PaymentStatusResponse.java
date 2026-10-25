package jhsoftware.paymentsflow.payment;

import java.util.UUID;

public record PaymentStatusResponse(
		UUID paymentId,
		String orderId,
		long amount,
		String currency,
		String status,
		LatestOperation latestOperation) {

	public record LatestOperation(UUID operationId, String type, String status) {
	}
}
