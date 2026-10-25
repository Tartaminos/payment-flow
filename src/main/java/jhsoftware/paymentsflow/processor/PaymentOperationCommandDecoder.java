package jhsoftware.paymentsflow.processor;

import java.time.Instant;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationCommandDecoder {

	PaymentOperationCommand decode(GenericRecord record) {
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
