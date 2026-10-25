package jhsoftware.paymentsflow.outbox;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class OutboxPublisher {

	private static final Logger logger = LoggerFactory.getLogger(OutboxPublisher.class);
	private static final String COMMAND_TYPE = "ExecutePaymentOperation";

	private final OutboxPublicationRepository repository;
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final Schema schema;
	private final OutboxMessagingProperties properties;
	private final Clock clock;

	OutboxPublisher(
			OutboxPublicationRepository repository,
			KafkaTemplate<String, Object> kafkaTemplate,
			Schema executePaymentOperationSchema,
			OutboxMessagingConfiguration.SchemaRegistryContract schemaRegistryContract,
			OutboxMessagingProperties properties,
			Clock clock) {
		this.repository = repository;
		this.kafkaTemplate = kafkaTemplate;
		this.schema = executePaymentOperationSchema;
		this.properties = properties;
		this.clock = clock;
	}

	@Scheduled(
			fixedDelayString = "${paymentflow.messaging.publisher.poll-interval:2s}",
			initialDelayString = "${paymentflow.messaging.publisher.poll-interval:2s}")
	void publishPending() {
		for (OutboxPublication publication : repository.findPending(properties.publisher().batchMax())) {
			if (!isValid(publication)) {
				logger.error("Outbox event {} has inconsistent persisted command identity", publication.messageId());
				continue;
			}

			try {
				kafkaTemplate.send(
						OutboxMessagingConfiguration.TOPIC,
						publication.paymentId().toString(),
						toRecord(publication))
						.get(properties.publisher().deliveryDeadline().toMillis(), TimeUnit.MILLISECONDS);

				if (!repository.markPublished(publication.messageId(), clock.instant())) {
					logger.warn("Outbox event {} was acknowledged but its pending state changed", publication.messageId());
				}
			}
			catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
				logger.warn("Outbox publication interrupted for event {}", publication.messageId());
				return;
			}
			catch (Exception exception) {
				logger.warn(
						"Outbox publication failed for event {} ({})",
						publication.messageId(),
						rootCause(exception).getClass().getSimpleName());
			}
		}
	}

	private Throwable rootCause(Throwable failure) {
		Throwable cause = failure;
		while (cause.getCause() != null && cause.getCause() != cause) {
			cause = cause.getCause();
		}
		return cause;
	}

	private boolean isValid(OutboxPublication publication) {
		return publication.messageId() != null
				&& publication.paymentId() != null
				&& publication.operationId() != null
				&& publication.authoritativePaymentId() != null
				&& publication.paymentId().equals(publication.authoritativePaymentId())
				&& COMMAND_TYPE.equals(publication.commandType())
				&& publication.correlationId() != null
				&& publication.occurredAt() != null;
	}

	private GenericRecord toRecord(OutboxPublication publication) {
		GenericRecord record = new GenericData.Record(schema);
		record.put("messageId", publication.messageId().toString());
		record.put("commandType", publication.commandType());
		record.put("operationId", publication.operationId().toString());
		record.put("correlationId", publication.correlationId().toString());
		record.put("occurredAt", publication.occurredAt().toEpochMilli());
		record.put(
				"causationId",
				publication.causationId() == null ? null : publication.causationId().toString());
		return record;
	}
}
