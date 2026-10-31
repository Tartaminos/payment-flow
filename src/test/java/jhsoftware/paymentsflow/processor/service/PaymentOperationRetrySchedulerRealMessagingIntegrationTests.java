package jhsoftware.paymentsflow.processor.service;

import jhsoftware.paymentsflow.processor.config.PaymentOperationQuarantineMessagingConfiguration;
import jhsoftware.paymentsflow.processor.config.PaymentOperationRetryMessagingConfiguration;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;
import jhsoftware.paymentsflow.processor.exception.UnresolvedRetryLineageException;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationCommandDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationQuarantinePublisher;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRawValueDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryEnvelopeDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryIdentity;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryPublisher;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetrySchedulerListener;
import jhsoftware.paymentsflow.processor.model.ScheduledRetry;
import jhsoftware.paymentsflow.processor.repository.ScheduledRetryRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@EnabledIfSystemProperty(named = "paymentflow.real-messaging-test", matches = "true")
@Testcontainers
@SpringBootTest(properties = {
		"spring.docker.compose.enabled=false",
		"spring.kafka.bootstrap-servers=localhost:9092",
		"spring.kafka.producer.properties.schema.registry.url=http://localhost:8082",
		"paymentflow.messaging.enabled=true",
		"paymentflow.messaging.schema-registry-url=http://localhost:8082",
		"paymentflow.messaging.publisher.poll-interval=1h"
})
class PaymentOperationRetrySchedulerRealMessagingIntegrationTests {

	private static final String SOURCE_TOPIC = "payment.operations.requested.v1";
	private static final String COMMAND_SUBJECT = "com.paymentflow.commands.ExecutePaymentOperation";

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	DataSource dataSource;

	@Autowired
	PaymentOperationRawValueDecoder rawValueDecoder;

	@Autowired
	PaymentOperationRetryEnvelopeDecoder retryEnvelopeDecoder;

	@Autowired
	PaymentOperationCommandDecoder commandDecoder;

	@Autowired
	RetryBackoffPolicy backoffPolicy;

	@Autowired
	PaymentOperationQuarantineHandoff quarantineHandoff;

	@Autowired
	PaymentOperationQuarantinePublisher quarantinePublisher;

	@Autowired
	PaymentOperationRetryPublisher retryPublisher;

	@Autowired
	PaymentOperationRetryScheduler scheduler;

	@Autowired
	Schema paymentOperationQuarantineSchema;

	@Autowired
	PaymentOperationQuarantineMessagingConfiguration.SchemaRegistryContract quarantineSchemaRegistryContract;

	@Autowired
	SchemaRegistryClient schemaRegistryClient;

	@Autowired
	Clock clock;

	@Autowired
	KafkaListenerEndpointRegistry listenerRegistry;

	@BeforeEach
	void stopLiveRetrySchedulerListener() {
		MessageListenerContainer container =
				listenerRegistry.getListenerContainer(PaymentOperationRetrySchedulerListener.LISTENER_ID);
		if (container != null && container.isRunning()) {
			container.stop();
		}
	}

	@BeforeEach
	void cleanDatabase() {
		jdbcTemplate.update("DELETE FROM quarantined_commands");
		jdbcTemplate.update("DELETE FROM scheduled_retries");
		jdbcTemplate.update("DELETE FROM outbox_events");
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
	}

	@Test
	void duplicateRetryRecordProducesASingleDurableScheduleWithStableRetryNumberAndEligibility() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		seedOutboxPublication(messageId, operationId, correlationId);
		byte[] sourceBytes = encodeExecutePaymentOperation(messageId, operationId, correlationId);
		PaymentOperationRetryEnvelopeDto envelope = retryEnvelope(21L, sourceBytes, operationId, correlationId, messageId);
		ConsumerRecord<byte[], Object> record = publishAndConsumeRetryRecord(envelope);
		UUID expectedRetryId = PaymentOperationRetryIdentity.retryId(SOURCE_TOPIC, envelope.sourcePartition(), 21L);

		boolean firstResult = scheduler.schedule(record);
		Map<String, Object> firstRow = fetchScheduledRetry(expectedRetryId);

		boolean secondResult = scheduler.schedule(record);
		Map<String, Object> secondRow = fetchScheduledRetry(expectedRetryId);

		assertThat(firstResult).isTrue();
		assertThat(secondResult).isTrue();
		assertThat(jdbcTemplate.queryForObject(
				"SELECT count(*) FROM scheduled_retries WHERE retry_id = ?", Long.class, expectedRetryId))
				.isEqualTo(1L);
		assertThat(firstRow.get("retry_number")).isEqualTo(1);
		assertThat(firstRow).isEqualTo(secondRow);
	}

	@Test
	void freshRepositoryInstanceAfterRestartObservesThePersistedPendingSchedule() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		seedOutboxPublication(messageId, operationId, correlationId);
		byte[] sourceBytes = encodeExecutePaymentOperation(messageId, operationId, correlationId);
		PaymentOperationRetryEnvelopeDto envelope = retryEnvelope(22L, sourceBytes, operationId, correlationId, messageId);
		ConsumerRecord<byte[], Object> record = publishAndConsumeRetryRecord(envelope);

		assertThat(scheduler.schedule(record)).isTrue();

		JdbcClient restartedJdbcClient = JdbcClient.create(dataSource);
		ScheduledRetryRepository restartedRepository = new ScheduledRetryRepository(restartedJdbcClient, backoffPolicy);
		int sourceSchemaId = rawValueDecoder.schemaId(sourceBytes);
		PaymentOperationRetryEnvelopeDto validatedEnvelope = retryEnvelopeDecoder.validateOriginal(
				envelope, commandDecoder.decode(rawValueDecoder.decode(SOURCE_TOPIC, sourceBytes)));

		ScheduledRetry observedAfterRestart = restartedRepository.persist(validatedEnvelope, sourceSchemaId);

		assertThat(observedAfterRestart.retryId()).isEqualTo(envelope.retryId());
		assertThat(observedAfterRestart.retryNumber()).isEqualTo(1);
		assertThat(observedAfterRestart.nextEligibleAt())
				.as("nominal 10s delay for retry 1, jittered within [80%, 120%] per ADR-0009")
				.isBetween(
						observedAfterRestart.scheduledAt().plusMillis(8000),
						observedAfterRestart.scheduledAt().plusMillis(12000));
		assertThat(jdbcTemplate.queryForObject(
				"SELECT count(*) FROM scheduled_retries WHERE retry_id = ?", Long.class, envelope.retryId()))
				.isEqualTo(1L);
	}

	@Test
	void scheduleStorageFailurePropagatesAndPersistsNoRow() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		seedOutboxPublication(messageId, operationId, correlationId);
		byte[] sourceBytes = encodeExecutePaymentOperation(messageId, operationId, correlationId);
		PaymentOperationRetryEnvelopeDto envelope = retryEnvelope(23L, sourceBytes, operationId, correlationId, messageId);
		ConsumerRecord<byte[], Object> record = publishAndConsumeRetryRecord(envelope);
		UUID expectedRetryId = PaymentOperationRetryIdentity.retryId(SOURCE_TOPIC, envelope.sourcePartition(), 23L);

		HikariDataSource brokenDataSource = brokenDataSource();
		try {
			ScheduledRetryRepository brokenRepository = new ScheduledRetryRepository(
					JdbcClient.create(brokenDataSource), backoffPolicy);
			PaymentOperationRetryScheduler brokenScheduler = new PaymentOperationRetryScheduler(
					rawValueDecoder, retryEnvelopeDecoder, commandDecoder, brokenRepository, quarantineHandoff);

			assertThatThrownBy(() -> brokenScheduler.schedule(record))
					.as("a durable-storage failure must propagate rather than silently reporting ack-eligibility")
					.isInstanceOf(DataAccessException.class);
		}
		finally {
			brokenDataSource.close();
		}

		assertThat(jdbcTemplate.queryForObject(
				"SELECT count(*) FROM scheduled_retries WHERE retry_id = ?", Long.class, expectedRetryId))
				.isEqualTo(0L);
	}

	@Test
	void unresolvedLineagePropagatesForDiagnosisWithoutPersistingAScheduleOrAcknowledging() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		byte[] sourceBytes = encodeExecutePaymentOperation(messageId, operationId, correlationId);
		PaymentOperationRetryEnvelopeDto envelope = retryEnvelope(24L, sourceBytes, operationId, correlationId, messageId);
		ConsumerRecord<byte[], Object> record = publishAndConsumeRetryRecord(envelope);
		UUID expectedRetryId = PaymentOperationRetryIdentity.retryId(SOURCE_TOPIC, envelope.sourcePartition(), 24L);

		assertThatThrownBy(() -> scheduler.schedule(record))
				.isInstanceOf(UnresolvedRetryLineageException.class);

		assertThat(jdbcTemplate.queryForObject(
				"SELECT count(*) FROM scheduled_retries WHERE retry_id = ?", Long.class, expectedRetryId))
				.isEqualTo(0L);
	}

	@Test
	void corruptedSourceCommandIsQuarantinedDurablyWhileRemainingAckEligible() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		byte[] corruptedSourceBytes = new byte[] {1, 2, 3};
		PaymentOperationRetryEnvelopeDto envelope = retryEnvelope(25L, corruptedSourceBytes, operationId, correlationId, messageId);
		ConsumerRecord<byte[], Object> record = publishAndConsumeRetryRecord(envelope);
		UUID expectedQuarantineId = PaymentOperationRetryIdentity.quarantineId(
				record.topic(), record.partition(), record.offset());

		try (KafkaConsumer<String, byte[]> dlqConsumer = topicConsumer("m5-03-dlq")) {
			dlqConsumer.subscribe(List.of(PaymentOperationQuarantineMessagingConfiguration.TOPIC));
			awaitAssignment(dlqConsumer);

			assertThat(scheduler.schedule(record)).isTrue();

			Map<String, Object> stored = jdbcTemplate.queryForMap("""
					SELECT failure_code, operation_id, correlation_id, source_message_id
					FROM quarantined_commands
					WHERE quarantine_id = ?
					""", expectedQuarantineId);
			assertThat(stored.get("failure_code")).isEqualTo(PoisonFailureCode.CORRUPTED_WIRE_FORMAT.name());
			assertThat(stored.get("operation_id")).isEqualTo(operationId);
			assertThat(stored.get("correlation_id")).isEqualTo(correlationId);
			assertThat(stored.get("source_message_id")).isEqualTo(messageId);
			assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM scheduled_retries", Long.class)).isZero();

			ConsumerRecord<String, byte[]> published = awaitRecord(dlqConsumer, expectedQuarantineId.toString());
			GenericRecord dlqEnvelope = decodeEnvelope(PaymentOperationQuarantineMessagingConfiguration.TOPIC, published.value());
			assertThat(dlqEnvelope.get("quarantineId").toString()).isEqualTo(expectedQuarantineId.toString());
			assertThat(dlqEnvelope.get("failureCode").toString())
					.isEqualTo(PoisonFailureCode.CORRUPTED_WIRE_FORMAT.name());
		}
	}

	private Map<String, Object> fetchScheduledRetry(UUID retryId) {
		return jdbcTemplate.queryForMap("""
				SELECT retry_number, scheduled_at, next_eligible_at
				FROM scheduled_retries
				WHERE retry_id = ?
				""", retryId);
	}

	private void seedOutboxPublication(UUID messageId, UUID operationId, UUID correlationId) {
		UUID paymentId = UUID.randomUUID();
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		jdbcTemplate.update("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, created_at, updated_at)
				VALUES (?, ?, ?, 5000, 'BRL', 'CREATED', ?, ?)
				""", paymentId, UUID.randomUUID(), "order-" + paymentId, now, now);
		jdbcTemplate.update("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key,
				     attempt_count, created_at, updated_at)
				VALUES (?, ?, 'AUTHORIZE', 'PENDING', 5000, ?, 0, ?, ?)
				""", operationId, paymentId, "psp-" + operationId, now, now);
		jdbcTemplate.update("""
				INSERT INTO outbox_events
				    (message_id, payment_id, operation_id, command_type, correlation_id,
				     occurred_at, status, created_at)
				VALUES (?, ?, ?, 'ExecutePaymentOperation', ?, ?, 'PENDING', ?)
				""", messageId, paymentId, operationId, correlationId, now, now);
	}

	private PaymentOperationRetryEnvelopeDto retryEnvelope(
			long offset, byte[] sourceBytes, UUID operationId, UUID correlationId, UUID sourceMessageId) {
		String key = UUID.randomUUID().toString();
		UUID retryId = PaymentOperationRetryIdentity.retryId(SOURCE_TOPIC, 5, offset);
		return new PaymentOperationRetryEnvelopeDto(
				retryId,
				SOURCE_TOPIC,
				5,
				offset,
				key.getBytes(StandardCharsets.UTF_8),
				sourceBytes,
				TechnicalFailureClassifier.DATA_ACCESS_UNAVAILABLE,
				Instant.now(),
				operationId,
				correlationId,
				sourceMessageId);
	}

	private ConsumerRecord<byte[], Object> publishAndConsumeRetryRecord(PaymentOperationRetryEnvelopeDto envelope) {
		try (KafkaConsumer<String, byte[]> retryConsumer = topicConsumer("m5-03-real-test")) {
			retryConsumer.subscribe(List.of(PaymentOperationRetryMessagingConfiguration.TOPIC));
			awaitAssignment(retryConsumer);

			assertThat(retryPublisher.publish(envelope)).isTrue();

			ConsumerRecord<String, byte[]> published = awaitRecord(retryConsumer, envelope.retryId().toString());
			return new ConsumerRecord<>(
					PaymentOperationRetryMessagingConfiguration.TOPIC,
					published.partition(),
					published.offset(),
					published.key().getBytes(StandardCharsets.UTF_8),
					published.value());
		}
	}

	private HikariDataSource brokenDataSource() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:postgresql://localhost:1/paymentflow-unreachable");
		config.setUsername("paymentflow");
		config.setPassword("paymentflow");
		config.setConnectionTimeout(1000);
		config.setInitializationFailTimeout(-1);
		config.setMaximumPoolSize(1);
		return new HikariDataSource(config);
	}

	private byte[] encodeExecutePaymentOperation(UUID messageId, UUID operationId, UUID correlationId) {
		Schema schema = loadExecutePaymentOperationSchema();
		GenericRecord record = new GenericData.Record(schema);
		record.put("messageId", messageId.toString());
		record.put("commandType", "ExecutePaymentOperation");
		record.put("operationId", operationId.toString());
		record.put("correlationId", correlationId.toString());
		record.put("occurredAt", Instant.now().toEpochMilli());
		record.put("causationId", null);

		Map<String, Object> serializerConfig = Map.of(
				"schema.registry.url", "http://localhost:8082",
				"auto.register.schemas", false,
				"use.latest.version", false,
				"value.subject.name.strategy", RecordNameStrategy.class.getName());
		try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient, serializerConfig)) {
			schemaRegistryClient.register(COMMAND_SUBJECT, new io.confluent.kafka.schemaregistry.avro.AvroSchema(schema));
			return serializer.serialize(SOURCE_TOPIC, record);
		}
		catch (java.io.IOException | io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException exception) {
			throw new IllegalStateException("Cannot register the ExecutePaymentOperation schema for the test", exception);
		}
	}

	private Schema loadExecutePaymentOperationSchema() {
		try (java.io.InputStream input = new org.springframework.core.io.ClassPathResource(
				"avro/com/paymentflow/commands/ExecutePaymentOperation.avsc").getInputStream()) {
			return new Schema.Parser().parse(input);
		}
		catch (java.io.IOException exception) {
			throw new IllegalStateException("Cannot load ExecutePaymentOperation Avro schema", exception);
		}
	}

	private GenericRecord decodeEnvelope(String topic, byte[] bytes) {
		Map<String, Object> deserializerConfig = Map.of(
				"schema.registry.url", "http://localhost:8082",
				"specific.avro.reader", false);
		try (io.confluent.kafka.serializers.KafkaAvroDeserializer deserializer =
				new io.confluent.kafka.serializers.KafkaAvroDeserializer(schemaRegistryClient, deserializerConfig)) {
			return (GenericRecord) deserializer.deserialize(topic, bytes);
		}
	}

	private KafkaConsumer<String, byte[]> topicConsumer(String groupPrefix) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupPrefix + "-" + UUID.randomUUID());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		return new KafkaConsumer<>(properties);
	}

	private void awaitAssignment(KafkaConsumer<String, byte[]> consumer) {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
			consumer.poll(Duration.ofMillis(250));
		}
		assertThat(consumer.assignment()).as("topic partition assignment").isNotEmpty();
	}

	private ConsumerRecord<String, byte[]> awaitRecord(KafkaConsumer<String, byte[]> consumer, String expectedKey) {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (System.nanoTime() < deadline) {
			for (ConsumerRecord<String, byte[]> candidate : consumer.poll(Duration.ofMillis(500))) {
				if (expectedKey.equals(candidate.key())) {
					return candidate;
				}
			}
		}
		throw new AssertionError("Expected record was not observed within 20 seconds");
	}
}
