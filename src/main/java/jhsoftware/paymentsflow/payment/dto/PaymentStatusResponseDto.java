package jhsoftware.paymentsflow.payment.dto;

import java.util.UUID;

public record PaymentStatusResponseDto(
		UUID paymentId,
		String orderId,
		long amount,
		String currency,
		String status,
		LatestOperation latestOperation) {

	public record LatestOperation(UUID operationId, String type, String status) {
	}
}
