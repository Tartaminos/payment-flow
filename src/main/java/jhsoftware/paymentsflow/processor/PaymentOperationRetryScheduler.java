package jhsoftware.paymentsflow.processor;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationRetryScheduler {

	private final PaymentOperationRawValueDecoder rawValueDecoder;
	private final PaymentOperationRetryEnvelopeDecoder retryEnvelopeDecoder;
	private final PaymentOperationCommandDecoder commandDecoder;
	private final ScheduledRetryRepository repository;
	private final PaymentOperationQuarantineHandoff quarantineHandoff;

	PaymentOperationRetryScheduler(
			PaymentOperationRawValueDecoder rawValueDecoder,
			PaymentOperationRetryEnvelopeDecoder retryEnvelopeDecoder,
			PaymentOperationCommandDecoder commandDecoder,
			ScheduledRetryRepository repository,
			PaymentOperationQuarantineHandoff quarantineHandoff) {
		this.rawValueDecoder = rawValueDecoder;
		this.retryEnvelopeDecoder = retryEnvelopeDecoder;
		this.commandDecoder = commandDecoder;
		this.repository = repository;
		this.quarantineHandoff = quarantineHandoff;
	}

	boolean schedule(ConsumerRecord<byte[], Object> record) {
		byte[] rawValue = record.value() == null ? new byte[0] : (byte[]) record.value();
		PaymentOperationRetryEnvelope retryEnvelope = null;
		try {
			GenericRecord retryRecord = rawValueDecoder.decode(
					record.topic(), rawValue, PaymentOperationRetryMessagingConfiguration.SUBJECT);
			retryEnvelope = retryEnvelopeDecoder.decode(retryRecord);
			validateRetryKey(record.key(), retryEnvelope);
			CorrelationContext.put(CorrelationContext.RETRY_ID, retryEnvelope.retryId());
			CorrelationContext.put(CorrelationContext.CORRELATION_ID, retryEnvelope.correlationId());
			CorrelationContext.put(CorrelationContext.OPERATION_ID, retryEnvelope.operationId());

			int sourceSchemaId = rawValueDecoder.schemaId(retryEnvelope.sourceRecordValue());
			GenericRecord sourceRecord = rawValueDecoder.decode(
					retryEnvelope.sourceTopic(), retryEnvelope.sourceRecordValue());
			validateOriginalKey(retryEnvelope.sourceRecordKey());
			PaymentOperationCommand originalCommand = commandDecoder.decode(sourceRecord);
			retryEnvelope = retryEnvelopeDecoder.validateOriginal(retryEnvelope, originalCommand);

			repository.persist(retryEnvelope, sourceSchemaId);
			return true;
		}
		catch (PoisonCommandException failure) {
			return quarantineHandoff.quarantine(
					record,
					rawValue,
					failure,
					identities(retryEnvelope));
		}
	}

	private void validateOriginalKey(byte[] keyBytes) {
		try {
			String key = StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(keyBytes))
					.toString();
			UUID paymentId = UUID.fromString(key);
			if (!paymentId.toString().equals(key)) {
				throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY);
			}
		}
		catch (IllegalArgumentException | CharacterCodingException exception) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY, exception);
		}
	}

	private void validateRetryKey(byte[] keyBytes, PaymentOperationRetryEnvelope envelope) {
		if (keyBytes == null) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY);
		}
		try {
			String key = StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(keyBytes))
					.toString();
			if (!envelope.retryId().toString().equals(key)) {
				throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY);
			}
		}
		catch (CharacterCodingException exception) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY, exception);
		}
	}

	private PaymentOperationQuarantineHandoff.Identities identities(PaymentOperationRetryEnvelope envelope) {
		return envelope == null
				? PaymentOperationQuarantineHandoff.Identities.none()
				: new PaymentOperationQuarantineHandoff.Identities(
						envelope.operationId(), envelope.correlationId(), envelope.sourceMessageId());
	}
}
