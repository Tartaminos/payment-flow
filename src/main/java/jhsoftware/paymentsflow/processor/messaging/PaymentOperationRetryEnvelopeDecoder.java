package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationRetryEnvelopeDecoder {

	public PaymentOperationRetryEnvelopeDto decode(GenericRecord record) {
		try {
			UUID retryId = requiredUuid(record, "retryId");
			String sourceTopic = requiredString(record, "sourceTopic");
			int sourcePartition = requiredNumber(record, "sourcePartition").intValue();
			long sourceOffset = requiredNumber(record, "sourceOffset").longValue();
			byte[] sourceRecordKey = requiredBytes(record, "sourceRecordKey");
			byte[] sourceRecordValue = requiredBytes(record, "sourceRecordValue");
			String failureClass = requiredString(record, "failureClass");
			Instant failedAt = Instant.ofEpochMilli(requiredNumber(record, "failedAt").longValue());
			UUID operationId = optionalUuid(record, "operationId");
			UUID correlationId = optionalUuid(record, "correlationId");
			UUID sourceMessageId = optionalUuid(record, "sourceMessageId");

			if (sourcePartition < 0 || sourceOffset < 0 || sourceTopic.length() > 255
					|| failureClass.length() > 64) {
				throw invalid();
			}
			UUID expectedRetryId = PaymentOperationRetryIdentity.retryId(
					sourceTopic, sourcePartition, sourceOffset);
			if (!retryId.equals(expectedRetryId)) {
				throw invalid();
			}

			return new PaymentOperationRetryEnvelopeDto(
					retryId, sourceTopic, sourcePartition, sourceOffset,
					sourceRecordKey, sourceRecordValue, failureClass, failedAt,
					operationId, correlationId, sourceMessageId);
		}
		catch (PoisonCommandException exception) {
			throw exception;
		}
		catch (RuntimeException exception) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_COMMAND_PAYLOAD, exception);
		}
	}

	public PaymentOperationRetryEnvelopeDto validateOriginal(
			PaymentOperationRetryEnvelopeDto envelope, PaymentOperationCommand originalCommand) {
		UUID operationId = checkedIdentity(envelope.operationId(), originalCommand.operationId());
		UUID correlationId = checkedIdentity(envelope.correlationId(), originalCommand.correlationId());
		UUID sourceMessageId = checkedIdentity(envelope.sourceMessageId(), originalCommand.messageId());
		return new PaymentOperationRetryEnvelopeDto(
				envelope.retryId(), envelope.sourceTopic(), envelope.sourcePartition(), envelope.sourceOffset(),
				envelope.sourceRecordKey(), envelope.sourceRecordValue(), envelope.failureClass(), envelope.failedAt(),
				operationId, correlationId, sourceMessageId);
	}

	private UUID checkedIdentity(UUID envelopeValue, UUID commandValue) {
		if (envelopeValue != null && !Objects.equals(envelopeValue, commandValue)) {
			throw invalid();
		}
		return envelopeValue == null ? commandValue : envelopeValue;
	}

	private UUID requiredUuid(GenericRecord record, String field) {
		UUID value = optionalUuid(record, field);
		if (value == null) {
			throw invalid();
		}
		return value;
	}

	private UUID optionalUuid(GenericRecord record, String field) {
		Object value = record.get(field);
		return value == null ? null : UUID.fromString(value.toString());
	}

	private String requiredString(GenericRecord record, String field) {
		Object value = record.get(field);
		String text = value == null ? null : value.toString();
		if (text == null || text.isBlank()) {
			throw invalid();
		}
		return text;
	}

	private Number requiredNumber(GenericRecord record, String field) {
		Object value = record.get(field);
		if (!(value instanceof Number number)) {
			throw invalid();
		}
		return number;
	}

	private byte[] requiredBytes(GenericRecord record, String field) {
		Object value = record.get(field);
		if (!(value instanceof ByteBuffer buffer)) {
			throw invalid();
		}
		ByteBuffer copy = buffer.duplicate();
		byte[] bytes = new byte[copy.remaining()];
		copy.get(bytes);
		return bytes;
	}

	private PoisonCommandException invalid() {
		return new PoisonCommandException(PoisonFailureCode.INVALID_COMMAND_PAYLOAD);
	}
}
