package jhsoftware.paymentsflow.processor;

import java.util.UUID;

record PspAuthorizeRequestBody(
		UUID pspOperationId,
		UUID merchantId,
		UUID paymentId,
		long amountMinor,
		String currency) {
}
