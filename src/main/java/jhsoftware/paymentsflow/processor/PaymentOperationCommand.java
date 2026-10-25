package jhsoftware.paymentsflow.processor;

import java.time.Instant;
import java.util.UUID;

record PaymentOperationCommand(
		UUID messageId,
		String commandType,
		UUID operationId,
		UUID correlationId,
		UUID causationId,
		Instant occurredAt) {
}
