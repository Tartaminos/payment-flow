package jhsoftware.paymentsflow.outbox;

import java.time.Instant;
import java.util.UUID;

record OutboxPublication(
		UUID messageId,
		UUID paymentId,
		UUID operationId,
		UUID authoritativePaymentId,
		String commandType,
		UUID correlationId,
		UUID causationId,
		Instant occurredAt) {
}
