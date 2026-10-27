package jhsoftware.paymentsflow.processor;

import java.util.UUID;

record DueScheduledRetry(
		UUID retryId,
		UUID operationId,
		UUID correlationId,
		UUID sourceMessageId,
		UUID dispatchMessageId,
		UUID paymentId) {
}
