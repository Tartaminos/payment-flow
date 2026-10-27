package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationQuarantineHandoff {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationQuarantineHandoff.class);

	private final PaymentOperationQuarantineRepository repository;
	private final PaymentOperationQuarantinePublisher publisher;
	private final Clock clock;

	PaymentOperationQuarantineHandoff(
			PaymentOperationQuarantineRepository repository,
			PaymentOperationQuarantinePublisher publisher,
			Clock clock) {
		this.repository = repository;
		this.publisher = publisher;
		this.clock = clock;
	}

	boolean quarantine(
			ConsumerRecord<byte[], Object> record,
			byte[] rawValue,
			PoisonCommandException failure,
			Identities identities) {
		UUID quarantineId = PaymentOperationRetryIdentity.quarantineId(
				record.topic(), record.partition(), record.offset());
		PaymentOperationQuarantineEnvelope envelope = new PaymentOperationQuarantineEnvelope(
				quarantineId,
				record.topic(),
				record.partition(),
				record.offset(),
				record.key() == null ? new byte[0] : record.key(),
				rawValue,
				failure.failureCode().name(),
				clock.instant(),
				identities.operationId(),
				identities.correlationId(),
				identities.sourceMessageId());

		repository.persist(envelope);
		if (!publisher.publish(envelope)) {
			throw failure;
		}
		logger.warn("Poison command at {}-{}-{} quarantined as {} with code {}",
				record.topic(), record.partition(), record.offset(), quarantineId, failure.failureCode());
		return true;
	}

	record Identities(UUID operationId, UUID correlationId, UUID sourceMessageId) {

		static Identities none() {
			return new Identities(null, null, null);
		}

		static Identities from(PaymentOperationCommand command) {
			return command == null
					? none()
					: new Identities(command.operationId(), command.correlationId(), command.messageId());
		}
	}
}
