package jhsoftware.paymentsflow.processor.command;

import java.time.Instant;
import java.util.UUID;

public record PaymentOperationCommand(
		UUID messageId,
		String commandType,
		UUID operationId,
		UUID correlationId,
		UUID causationId,
		Instant occurredAt) {
}
