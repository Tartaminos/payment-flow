package jhsoftware.paymentsflow.processor;

import java.util.UUID;

record PspVoidRequestBody(
		UUID pspOperationId,
		UUID merchantId,
		UUID paymentId,
		long amountMinor,
		String currency,
		UUID authorizationPspOperationId) {
}
