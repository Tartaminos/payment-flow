package jhsoftware.paymentsflow.processor;

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
	private final Clock clock;

	PaymentOperationTechnicalRetryHandoff(
			PaymentOperationRawValueDecoder rawValueDecoder,
			PaymentOperationCommandDecoder commandDecoder,
			PaymentOperationProcessor processor,
			TechnicalFailureClassifier classifier,
			PaymentOperationRetryPublisher retryPublisher,
			Clock clock) {
		this.rawValueDecoder = rawValueDecoder;
		this.commandDecoder = commandDecoder;
		this.processor = processor;
		this.classifier = classifier;
		this.retryPublisher = retryPublisher;
		this.clock = clock;
	}

	boolean handle(ConsumerRecord<String, Object> record) {
		byte[] rawValue = (byte[]) record.value();
		GenericRecord decoded = null;
		try {
			decoded = rawValueDecoder.decode(record.topic(), rawValue);
			return processor.process(record.key(), decoded);
		}
		catch (RuntimeException exception) {
			Optional<String> failureClass = classifier.classifyTransient(exception);
			if (failureClass.isEmpty()) {
				throw exception;
			}
			return handOffToRetryTopic(record, rawValue, decoded, failureClass.get(), exception);
		}
	}

	private boolean handOffToRetryTopic(
			ConsumerRecord<String, Object> record,
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

	private byte[] sourceKeyBytes(String key) {
		return key == null ? new byte[0] : key.getBytes(StandardCharsets.UTF_8);
	}
}
