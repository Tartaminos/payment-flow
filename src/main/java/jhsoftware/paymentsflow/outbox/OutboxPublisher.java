package jhsoftware.paymentsflow.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
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
	private static final int MAX_BACKOFF_SHIFT = 30;

	private final OutboxPublicationRepository repository;
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final Schema schema;
	private final OutboxMessagingProperties properties;
	private final Clock clock;
	private final UUID workerId;

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
		this.workerId = UUID.randomUUID();
	}

	@Scheduled(
			fixedDelayString = "${paymentflow.messaging.publisher.poll-interval:2s}",
			initialDelayString = "${paymentflow.messaging.publisher.poll-interval:2s}")
	void publishPending() {
		Instant now = clock.instant();
		Instant leaseExpiresAt = now.plus(properties.publisher().claimLease());

		for (OutboxPublication publication : repository.claimPending(
				properties.publisher().batchMax(), workerId, now, leaseExpiresAt)) {
			if (!isValid(publication)) {
				logger.error("Outbox event {} has inconsistent persisted command identity", publication.messageId());
				release(publication);
				continue;
			}

			try {
				kafkaTemplate.send(
						OutboxMessagingConfiguration.TOPIC,
						publication.paymentId().toString(),
						toRecord(publication))
						.get(properties.publisher().deliveryDeadline().toMillis(), TimeUnit.MILLISECONDS);

				if (!repository.markPublished(publication.messageId(), workerId, clock.instant())) {
					logger.warn(
							"Outbox event {} was acknowledged but its claim was lost to another worker",
							publication.messageId());
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
				release(publication);
			}
		}
	}

	private void release(OutboxPublication publication) {
		Instant now = clock.instant();
		if (!repository.releaseForRetry(publication.messageId(), workerId, nextEligibleAt(publication, now))) {
			logger.warn(
					"Outbox event {} could not be released because its claim was lost to another worker",
					publication.messageId());
		}
	}

	private Instant nextEligibleAt(OutboxPublication publication, Instant now) {
		Duration initial = properties.publisher().retryBackoffInitial();
		Duration max = properties.publisher().retryBackoffMax();
		int shift = Math.min(Math.max(publication.attemptCount() - 1, 0), MAX_BACKOFF_SHIFT);
		Duration backoff = initial.multipliedBy(1L << shift);
		if (backoff.compareTo(max) > 0) {
			backoff = max;
		}
		return now.plus(backoff);
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
