package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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
class PaymentOperationTechnicalRetryHandoffRealMessagingIntegrationTests {

	private static final String SOURCE_TOPIC = "payment.operations.requested.v1";
	private static final String COMMAND_SUBJECT = "com.paymentflow.commands.ExecutePaymentOperation";

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	PaymentOperationRawValueDecoder rawValueDecoder;

	@Autowired
	PaymentOperationCommandDecoder commandDecoder;

	@Autowired
	TechnicalFailureClassifier classifier;

	@Autowired
	PaymentOperationRetryPublisher retryPublisher;

	@Autowired
	PaymentOperationQuarantineRepository quarantineRepository;

	@Autowired
	PaymentOperationQuarantinePublisher quarantinePublisher;

	@Autowired
	Schema paymentOperationRetrySchema;

	@Autowired
	Schema paymentOperationQuarantineSchema;

	@Autowired
	PaymentOperationRetryMessagingConfiguration.SchemaRegistryContract retrySchemaRegistryContract;

	@Autowired
	PaymentOperationQuarantineMessagingConfiguration.SchemaRegistryContract quarantineSchemaRegistryContract;

	@Autowired
	SchemaRegistryClient schemaRegistryClient;

	@Autowired
	Clock clock;

	@BeforeEach
	void cleanDatabase() {
		jdbcTemplate.update("DELETE FROM quarantined_commands");
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
	}

	@Test
	void corruptedRecordIsPersistedAndPublishedToDlqBeforeBecomingAckEligible() {
		byte[] sourceKey = new byte[] {(byte) 0x80, 1, 2};
		byte[] sourceValue = new byte[] {1, 2, 3};
		ConsumerRecord<byte[], Object> record = new ConsumerRecord<>(
				SOURCE_TOPIC, 1, 401L, sourceKey, sourceValue);
		PaymentOperationTechnicalRetryHandoff handoff = new PaymentOperationTechnicalRetryHandoff(
				rawValueDecoder, commandDecoder, mock(PaymentOperationProcessor.class), classifier, retryPublisher,
				new PaymentOperationQuarantineHandoff(quarantineRepository, quarantinePublisher, clock), clock);
		UUID quarantineId = PaymentOperationRetryIdentity.quarantineId(SOURCE_TOPIC, 1, 401L);

		try (KafkaConsumer<String, byte[]> dlqConsumer = topicConsumer("m5-02-dlq")) {
			dlqConsumer.subscribe(List.of(PaymentOperationQuarantineMessagingConfiguration.TOPIC));
			awaitAssignment(dlqConsumer);

			assertThat(handoff.handle(record)).isTrue();

			Map<String, Object> stored = jdbcTemplate.queryForMap("""
					SELECT source_record_key, source_record_value, failure_code
					FROM quarantined_commands
					WHERE quarantine_id = ?
					""", quarantineId);
			assertThat((byte[]) stored.get("source_record_key")).isEqualTo(sourceKey);
			assertThat((byte[]) stored.get("source_record_value")).isEqualTo(sourceValue);
			assertThat(stored.get("failure_code")).isEqualTo(PoisonFailureCode.CORRUPTED_WIRE_FORMAT.name());

			ConsumerRecord<String, byte[]> published = awaitRecord(dlqConsumer, quarantineId.toString());
			GenericRecord envelope = decodeEnvelope(
					PaymentOperationQuarantineMessagingConfiguration.TOPIC, published.value());
			assertThat(envelope.get("quarantineId").toString()).isEqualTo(quarantineId.toString());
			assertThat(envelope.get("failureCode").toString())
					.isEqualTo(PoisonFailureCode.CORRUPTED_WIRE_FORMAT.name());
			assertThat(bytes(envelope, "sourceRecordKey")).isEqualTo(sourceKey);
			assertThat(bytes(envelope, "sourceRecordValue")).isEqualTo(sourceValue);
			assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM payment_operations", Long.class)).isZero();
		}
	}

	@Test
	void unreachableDlqBrokerLeavesPersistedPoisonRecordIneligibleForAcknowledgment() {
		DefaultKafkaProducerFactory<String, Object> brokenProducerFactory = brokenProducerFactory();
		KafkaTemplate<String, Object> brokenKafkaTemplate = new KafkaTemplate<>(brokenProducerFactory);
		PaymentOperationQuarantinePublisher brokenPublisher = new PaymentOperationQuarantinePublisher(
				brokenKafkaTemplate, paymentOperationQuarantineSchema, quarantineSchemaRegistryContract);
		PaymentOperationTechnicalRetryHandoff handoff = new PaymentOperationTechnicalRetryHandoff(
				rawValueDecoder, commandDecoder, mock(PaymentOperationProcessor.class), classifier, retryPublisher,
				new PaymentOperationQuarantineHandoff(quarantineRepository, brokenPublisher, clock), clock);
		ConsumerRecord<byte[], Object> record = new ConsumerRecord<>(
				SOURCE_TOPIC, 2, 402L, new byte[] {9, 8}, new byte[] {1, 2, 3});
		UUID quarantineId = PaymentOperationRetryIdentity.quarantineId(SOURCE_TOPIC, 2, 402L);

		try {
			assertThatThrownBy(() -> handoff.handle(record))
					.isInstanceOfSatisfying(PoisonCommandException.class,
							exception -> assertThat(exception.failureCode())
									.isEqualTo(PoisonFailureCode.CORRUPTED_WIRE_FORMAT));
			assertThat(jdbcTemplate.queryForObject(
					"SELECT count(*) FROM quarantined_commands WHERE quarantine_id = ?", Long.class, quarantineId))
					.isEqualTo(1L);
		}
		finally {
			brokenProducerFactory.destroy();
		}
	}

	@Test
	void transientFailureHandoffPublishesAcknowledgedRetryRecordBeforeBecomingAckEligible() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		byte[] sourceBytes = encodeExecutePaymentOperation(messageId, operationId, correlationId);
		String key = UUID.randomUUID().toString();
		ConsumerRecord<byte[], Object> record = new ConsumerRecord<>(
				SOURCE_TOPIC, 3, 99L, key.getBytes(StandardCharsets.UTF_8), sourceBytes);

		PaymentOperationProcessor failingProcessor = mock(PaymentOperationProcessor.class);
		when(failingProcessor.process(eq(key), any())).thenThrow(
				new org.springframework.dao.DataAccessResourceFailureException("database unavailable"));
		PaymentOperationTechnicalRetryHandoff handoff = new PaymentOperationTechnicalRetryHandoff(
				rawValueDecoder, commandDecoder, failingProcessor, classifier, retryPublisher,
				new PaymentOperationQuarantineHandoff(
						mock(PaymentOperationQuarantineRepository.class),
						mock(PaymentOperationQuarantinePublisher.class), clock), clock);

		try (KafkaConsumer<String, byte[]> retryConsumer = retryTopicConsumer()) {
			retryConsumer.subscribe(List.of(PaymentOperationRetryMessagingConfiguration.TOPIC));
			awaitAssignment(retryConsumer);

			boolean ackEligible = handoff.handle(record);

			assertThat(ackEligible)
					.as("handoff only becomes ack-eligible after the retry-topic publish returns a broker acknowledgment")
					.isTrue();
			assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM payment_operations", Long.class))
					.as("the transient failure handoff must not create any PaymentOperation row")
					.isZero();

			UUID expectedRetryId = PaymentOperationRetryIdentity.retryId(SOURCE_TOPIC, 3, 99L);
			ConsumerRecord<String, byte[]> published = awaitRecord(retryConsumer, expectedRetryId.toString());
			GenericRecord decodedEnvelope = decodeRetryEnvelope(published.value());
			assertThat(decodedEnvelope.get("retryId").toString()).isEqualTo(expectedRetryId.toString());
			assertThat(decodedEnvelope.get("sourceTopic").toString()).isEqualTo(SOURCE_TOPIC);
			assertThat(decodedEnvelope.get("sourcePartition")).isEqualTo(3);
			assertThat(decodedEnvelope.get("sourceOffset")).isEqualTo(99L);
			assertThat(((java.nio.ByteBuffer) decodedEnvelope.get("sourceRecordValue")).array()).isEqualTo(sourceBytes);
			assertThat(decodedEnvelope.get("operationId").toString()).isEqualTo(operationId.toString());
			assertThat(decodedEnvelope.get("correlationId").toString()).isEqualTo(correlationId.toString());
			assertThat(decodedEnvelope.get("sourceMessageId").toString()).isEqualTo(messageId.toString());
			assertThat(decodedEnvelope.get("failureClass").toString()).isEqualTo(TechnicalFailureClassifier.DATA_ACCESS_UNAVAILABLE);
		}
	}

	@Test
	void redeliveredSourceCoordinatesProduceTheSameRetryIdOnRepublish() {
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		byte[] sourceBytes = encodeExecutePaymentOperation(messageId, operationId, correlationId);
		String key = UUID.randomUUID().toString();
		ConsumerRecord<byte[], Object> record = new ConsumerRecord<>(
				SOURCE_TOPIC, 7, 12L, key.getBytes(StandardCharsets.UTF_8), sourceBytes);

		PaymentOperationProcessor failingProcessor = mock(PaymentOperationProcessor.class);
		when(failingProcessor.process(eq(key), any())).thenThrow(
				new org.springframework.dao.DataAccessResourceFailureException("database unavailable"));
		PaymentOperationTechnicalRetryHandoff handoff = new PaymentOperationTechnicalRetryHandoff(
				rawValueDecoder, commandDecoder, failingProcessor, classifier, retryPublisher,
				new PaymentOperationQuarantineHandoff(
						mock(PaymentOperationQuarantineRepository.class),
						mock(PaymentOperationQuarantinePublisher.class), clock), clock);

		try (KafkaConsumer<String, byte[]> retryConsumer = retryTopicConsumer()) {
			retryConsumer.subscribe(List.of(PaymentOperationRetryMessagingConfiguration.TOPIC));
			awaitAssignment(retryConsumer);

			assertThat(handoff.handle(record)).isTrue();
			assertThat(handoff.handle(record)).isTrue();

			UUID expectedRetryId = PaymentOperationRetryIdentity.retryId(SOURCE_TOPIC, 7, 12L);
			List<ConsumerRecord<String, byte[]>> published = awaitRecords(retryConsumer, expectedRetryId.toString(), 2);
			assertThat(published).hasSize(2);
			assertThat(published).extracting(ConsumerRecord::key)
					.containsExactly(expectedRetryId.toString(), expectedRetryId.toString());
		}
	}

	@Test
	void publishFailureAgainstAnUnreachableBrokerReturnsFalseAndNeverAcknowledges() {
		DefaultKafkaProducerFactory<String, Object> brokenProducerFactory = brokenProducerFactory();
		KafkaTemplate<String, Object> brokenKafkaTemplate = new KafkaTemplate<>(brokenProducerFactory);
		PaymentOperationRetryPublisher brokenPublisher = new PaymentOperationRetryPublisher(
				brokenKafkaTemplate, paymentOperationRetrySchema, retrySchemaRegistryContract);

		PaymentOperationRetryEnvelope envelope = new PaymentOperationRetryEnvelope(
				UUID.randomUUID(),
				SOURCE_TOPIC,
				0,
				0L,
				"key".getBytes(),
				new byte[] {1, 2, 3},
				TechnicalFailureClassifier.DATA_ACCESS_UNAVAILABLE,
				Instant.now(),
				null,
				null,
				null);

		boolean published = brokenPublisher.publish(envelope);

		assertThat(published)
				.as("an unreachable broker must fail the publish, which must prevent the source offset from advancing")
				.isFalse();

		brokenProducerFactory.destroy();
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

	private GenericRecord decodeRetryEnvelope(byte[] bytes) {
		return decodeEnvelope(PaymentOperationRetryMessagingConfiguration.TOPIC, bytes);
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

	private byte[] bytes(GenericRecord record, String field) {
		java.nio.ByteBuffer buffer = ((java.nio.ByteBuffer) record.get(field)).duplicate();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return bytes;
	}

	private DefaultKafkaProducerFactory<String, Object> brokenProducerFactory() {
		Map<String, Object> config = new java.util.HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		config.put(ProducerConfig.ACKS_CONFIG, "all");
		config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500);
		config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000);
		config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
		config.put("schema.registry.url", "http://localhost:8082");
		config.put("auto.register.schemas", false);
		config.put("use.latest.version", false);
		config.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
		return new DefaultKafkaProducerFactory<>(config);
	}

	private KafkaConsumer<String, byte[]> retryTopicConsumer() {
		return topicConsumer("m5-01-real-test");
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
		assertThat(consumer.assignment()).as("retry topic partition assignment").isNotEmpty();
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
		throw new AssertionError("Expected retry record was not observed within 20 seconds");
	}

	private List<ConsumerRecord<String, byte[]>> awaitRecords(
			KafkaConsumer<String, byte[]> consumer, String expectedKey, int expectedCount) {
		List<ConsumerRecord<String, byte[]>> observed = new java.util.ArrayList<>();
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (observed.size() < expectedCount && System.nanoTime() < deadline) {
			for (ConsumerRecord<String, byte[]> candidate : consumer.poll(Duration.ofMillis(500))) {
				if (expectedKey.equals(candidate.key())) {
					observed.add(candidate);
				}
			}
		}
		return observed;
	}
}
