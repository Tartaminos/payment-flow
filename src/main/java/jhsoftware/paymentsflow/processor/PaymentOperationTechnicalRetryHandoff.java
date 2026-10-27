package jhsoftware.paymentsflow.processor;

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

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationTechnicalRetryHandoff {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationTechnicalRetryHandoff.class);

	private final PaymentOperationRawValueDecoder rawValueDecoder;
	private final PaymentOperationCommandDecoder commandDecoder;
	private final PaymentOperationProcessor processor;
	private final TechnicalFailureClassifier classifier;
	private final PaymentOperationRetryPublisher retryPublisher;
	private final PaymentOperationQuarantineRepository quarantineRepository;
	private final PaymentOperationQuarantinePublisher quarantinePublisher;
	private final Clock clock;

	PaymentOperationTechnicalRetryHandoff(
			PaymentOperationRawValueDecoder rawValueDecoder,
			PaymentOperationCommandDecoder commandDecoder,
			PaymentOperationProcessor processor,
			TechnicalFailureClassifier classifier,
			PaymentOperationRetryPublisher retryPublisher,
			PaymentOperationQuarantineRepository quarantineRepository,
			PaymentOperationQuarantinePublisher quarantinePublisher,
			Clock clock) {
		this.rawValueDecoder = rawValueDecoder;
		this.commandDecoder = commandDecoder;
		this.processor = processor;
		this.classifier = classifier;
		this.retryPublisher = retryPublisher;
		this.quarantineRepository = quarantineRepository;
		this.quarantinePublisher = quarantinePublisher;
		this.clock = clock;
	}

	boolean handle(ConsumerRecord<byte[], Object> record) {
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
		UUID quarantineId = PaymentOperationRetryIdentity.quarantineId(
				record.topic(), record.partition(), record.offset());
		PaymentOperationCommand command = bestEffortDecodedCommand(decoded);
		PaymentOperationQuarantineEnvelope envelope = new PaymentOperationQuarantineEnvelope(
				quarantineId,
				record.topic(),
				record.partition(),
				record.offset(),
				sourceKeyBytes(record.key()),
				rawValue,
				failure.failureCode().name(),
				clock.instant(),
				command == null ? null : command.operationId(),
				command == null ? null : command.correlationId(),
				command == null ? null : command.messageId());

		quarantineRepository.persist(envelope);
		if (!quarantinePublisher.publish(envelope)) {
			throw failure;
		}
		logger.warn("Poison command at {}-{}-{} quarantined as {} with code {}",
				record.topic(), record.partition(), record.offset(), quarantineId, failure.failureCode());
		return true;
	}

	private boolean handOffToRetryTopic(
			ConsumerRecord<byte[], Object> record,
			byte[] rawValue,
			GenericRecord decoded,
			String failureClass,
			RuntimeException failure) {
		UUID retryId = PaymentOperationRetryIdentity.retryId(record.topic(), record.partition(), record.offset());
		PaymentOperationCommand command = bestEffortCommand(record.topic(), rawValue, decoded);

		PaymentOperationRetryEnvelope envelope = new PaymentOperationRetryEnvelope(
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
