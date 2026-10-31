package jhsoftware.paymentsflow.processor.service;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationCommandDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRawValueDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryIdentity;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryPublisher;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationTechnicalRetryHandoff {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationTechnicalRetryHandoff.class);

	private final PaymentOperationRawValueDecoder rawValueDecoder;
	private final PaymentOperationCommandDecoder commandDecoder;
	private final PaymentOperationProcessor processor;
	private final TechnicalFailureClassifier classifier;
	private final PaymentOperationRetryPublisher retryPublisher;
	private final PaymentOperationQuarantineHandoff quarantineHandoff;
	private final Clock clock;

	PaymentOperationTechnicalRetryHandoff(
			PaymentOperationRawValueDecoder rawValueDecoder,
			PaymentOperationCommandDecoder commandDecoder,
			PaymentOperationProcessor processor,
			TechnicalFailureClassifier classifier,
			PaymentOperationRetryPublisher retryPublisher,
			PaymentOperationQuarantineHandoff quarantineHandoff,
			Clock clock) {
		this.rawValueDecoder = rawValueDecoder;
		this.commandDecoder = commandDecoder;
		this.processor = processor;
		this.classifier = classifier;
		this.retryPublisher = retryPublisher;
		this.quarantineHandoff = quarantineHandoff;
		this.clock = clock;
	}

	public boolean handle(ConsumerRecord<byte[], Object> record) {
		byte[] rawValue = record.value() == null ? new byte[0] : (byte[]) record.value();
		GenericRecord decoded = null;
		try {
			decoded = rawValueDecoder.decode(record.topic(), rawValue);
			return processor.process(canonicalKey(record.key()), decoded);
		}
		catch (RuntimeException exception) {
			Optional<String> failureClass = classifier.classifyTransient(exception);
			if (failureClass.isPresent()) {
				return handOffToRetryTopic(record, rawValue, decoded, failureClass.get(), exception);
			}
			if (exception instanceof PoisonCommandException poison) {
				return quarantine(record, rawValue, decoded, poison);
			}
			throw exception;
		}
	}

	private boolean quarantine(
			ConsumerRecord<byte[], Object> record,
			byte[] rawValue,
			GenericRecord decoded,
			PoisonCommandException failure) {
		PaymentOperationCommand command = bestEffortDecodedCommand(decoded);
		return quarantineHandoff.quarantine(
				record, rawValue, failure, PaymentOperationQuarantineHandoff.Identities.from(command));
	}

	private boolean handOffToRetryTopic(
			ConsumerRecord<byte[], Object> record,
			byte[] rawValue,
			GenericRecord decoded,
			String failureClass,
			RuntimeException failure) {
		UUID retryId = PaymentOperationRetryIdentity.retryId(record.topic(), record.partition(), record.offset());
		PaymentOperationCommand command = bestEffortCommand(record.topic(), rawValue, decoded);

		CorrelationContext.put(CorrelationContext.RETRY_ID, retryId);
		if (command != null) {
			CorrelationContext.put(CorrelationContext.CORRELATION_ID, command.correlationId());
			CorrelationContext.put(CorrelationContext.OPERATION_ID, command.operationId());
		}

		PaymentOperationRetryEnvelopeDto envelope = new PaymentOperationRetryEnvelopeDto(
				retryId,
				record.topic(),
				record.partition(),
				record.offset(),
				sourceKeyBytes(record.key()),
				rawValue,
				failureClass,
				clock.instant(),
				command == null ? null : command.operationId(),
				command == null ? null : command.correlationId(),
				command == null ? null : command.messageId());

		if (!retryPublisher.publish(envelope)) {
			throw failure;
		}
		logger.warn(
				"Transient failure for {}-{}-{} handed off to the technical retry topic as {}",
				record.topic(),
				record.partition(),
				record.offset(),
				retryId);
		return true;
	}

	private PaymentOperationCommand bestEffortCommand(String topic, byte[] rawValue, GenericRecord decoded) {
		try {
			GenericRecord commandRecord = decoded == null ? rawValueDecoder.decode(topic, rawValue) : decoded;
			return commandDecoder.decode(commandRecord);
		}
		catch (RuntimeException exception) {
			return null;
		}
	}

	private PaymentOperationCommand bestEffortDecodedCommand(GenericRecord decoded) {
		if (decoded == null) {
			return null;
		}
		try {
			return commandDecoder.decodeForDiagnostics(decoded);
		}
		catch (RuntimeException exception) {
			return null;
		}
	}

	private byte[] sourceKeyBytes(byte[] key) {
		return key == null ? new byte[0] : key;
	}

	private String canonicalKey(byte[] key) {
		if (key == null) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY);
		}
		try {
			return StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(key))
					.toString();
		}
		catch (CharacterCodingException exception) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY, exception);
		}
	}
}
