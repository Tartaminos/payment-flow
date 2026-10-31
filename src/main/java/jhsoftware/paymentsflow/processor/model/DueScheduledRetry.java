package jhsoftware.paymentsflow.processor.model;

import java.util.UUID;

public record DueScheduledRetry(
		UUID retryId,
		UUID operationId,
		UUID correlationId,
		UUID sourceMessageId,
		UUID dispatchMessageId,
		UUID paymentId) {
}
