package jhsoftware.paymentsflow.processor.dto;

import java.util.UUID;

public record PspCaptureRequestDto(
		UUID pspOperationId,
		UUID merchantId,
		UUID paymentId,
		long amountMinor,
		String currency,
		UUID authorizationPspOperationId) {
}
