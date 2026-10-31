package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;

import java.time.Instant;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationCommandDecoder {

	public PaymentOperationCommand decode(GenericRecord record) {
		try {
			PaymentOperationCommand command = decodeForDiagnostics(record);
			if (command.messageId() == null || command.operationId() == null || command.correlationId() == null
					|| command.occurredAt() == null) {
				throw new PoisonCommandException(PoisonFailureCode.INVALID_COMMAND_PAYLOAD);
			}
			if (!"ExecutePaymentOperation".equals(command.commandType())) {
				throw new PoisonCommandException(PoisonFailureCode.UNSUPPORTED_COMMAND_TYPE);
			}
			return command;
		}
		catch (PoisonCommandException exception) {
			throw exception;
		}
		catch (RuntimeException exception) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_COMMAND_PAYLOAD, exception);
		}
	}

	public PaymentOperationCommand decodeForDiagnostics(GenericRecord record) {
		return new PaymentOperationCommand(
				uuidOf(record, "messageId"),
				stringOf(record, "commandType"),
				uuidOf(record, "operationId"),
				uuidOf(record, "correlationId"),
				uuidOf(record, "causationId"),
				instantOf(record, "occurredAt"));
	}

	private UUID uuidOf(GenericRecord record, String field) {
		Object value = record.get(field);
		return value == null ? null : UUID.fromString(value.toString());
	}

	private String stringOf(GenericRecord record, String field) {
		Object value = record.get(field);
		return value == null ? null : value.toString();
	}

	private Instant instantOf(GenericRecord record, String field) {
		Object value = record.get(field);
		return value == null ? null : Instant.ofEpochMilli((Long) value);
	}
}
