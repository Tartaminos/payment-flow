package jhsoftware.paymentsflow.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.jayway.jsonpath.JsonPath;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@EnabledIfSystemProperty(named = "paymentflow.real-messaging-test", matches = "true")
@Testcontainers
@SpringBootTest(properties = {
		"spring.docker.compose.enabled=false",
		"spring.kafka.bootstrap-servers=localhost:9092",
		"spring.kafka.producer.properties.schema.registry.url=http://localhost:8081",
		"paymentflow.messaging.enabled=true",
		"paymentflow.messaging.schema-registry-url=http://localhost:8081",
		"paymentflow.messaging.publisher.poll-interval=1h"
})
@AutoConfigureMockMvc
class OutboxRealMessagingIntegrationTests {

	private static final String TOPIC = "payment.operations.requested.v1";
	private static final String SUBJECT = "com.paymentflow.commands.ExecutePaymentOperation";

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	MockMvc mockMvc;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	OutboxPublisher publisher;

	@Autowired
	SchemaRegistryClient schemaRegistryClient;

	@BeforeEach
	void cleanDatabase() {
		jdbcTemplate.update("DELETE FROM outbox_events");
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM idempotency_keys");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
	}

	@Test
	void committedAdmissionPublishesDecodableCommandAndRecordsBrokerAcknowledgement() throws Exception {
		MvcResult response = mockMvc.perform(post("/payments")
				.header("Idempotency-Key", "real-messaging-" + UUID.randomUUID())
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"orderId\":\"real-150\",\"amount\":50000,\"currency\":\"BRL\"}"))
				.andExpect(status().isAccepted())
				.andReturn();
		UUID paymentId = UUID.fromString(JsonPath.read(
				response.getResponse().getContentAsString(), "$.paymentId"));
		Map<String, Object> outbox = jdbcTemplate.queryForMap("SELECT * FROM outbox_events");
		UUID messageId = (UUID) outbox.get("message_id");

		try (KafkaConsumer<byte[], Object> consumer = consumer()) {
			consumer.subscribe(List.of(TOPIC));
			awaitAssignment(consumer);
			publisher.publishPending();

			ConsumerRecord<byte[], Object> published = awaitMessage(consumer, messageId);
			assertThat(new String(published.key(), StandardCharsets.UTF_8))
					.isEqualTo(paymentId.toString())
					.hasSize(36);
			assertThat(published.key()).hasSize(36);

			GenericRecord command = (GenericRecord) published.value();
			assertThat(command.getSchema().getFullName()).isEqualTo(SUBJECT);
			assertThat(command.getSchema().getFields()).extracting(field -> field.name())
					.containsExactly("messageId", "commandType", "operationId", "correlationId", "occurredAt", "causationId");
			assertThat(command.get("messageId").toString()).isEqualTo(messageId.toString());
			assertThat(command.get("operationId").toString()).isEqualTo(outbox.get("operation_id").toString());
			assertThat(command.get("correlationId").toString()).isEqualTo(outbox.get("correlation_id").toString());
			assertThat(command.get("causationId")).isNull();
			assertThat(command.getSchema().getField("paymentId")).isNull();
		}

		Map<String, Object> delivery = jdbcTemplate.queryForMap(
				"SELECT status, published_at FROM outbox_events WHERE message_id = ?", messageId);
		assertThat(delivery).containsEntry("status", "PUBLISHED");
		assertThat(delivery.get("published_at")).isNotNull();
		assertThat(schemaRegistryClient.getAllSubjects()).contains(SUBJECT);
		assertThat(schemaRegistryClient.getCompatibility(SUBJECT)).isEqualTo("FULL_TRANSITIVE");
		assertTopicContract();
	}

	private KafkaConsumer<byte[], Object> consumer() {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "m2-real-test-" + UUID.randomUUID());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		properties.put("schema.registry.url", "http://localhost:8081");
		return new KafkaConsumer<>(properties);
	}

	private void awaitAssignment(KafkaConsumer<byte[], Object> consumer) {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
			consumer.poll(Duration.ofMillis(250));
		}
		assertThat(consumer.assignment()).as("Kafka partition assignment").isNotEmpty();
	}

	private ConsumerRecord<byte[], Object> awaitMessage(
			KafkaConsumer<byte[], Object> consumer,
			UUID expectedMessageId) {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (System.nanoTime() < deadline) {
			for (ConsumerRecord<byte[], Object> record : consumer.poll(Duration.ofMillis(500))) {
				GenericRecord command = (GenericRecord) record.value();
				if (expectedMessageId.toString().equals(command.get("messageId").toString())) {
					return record;
				}
			}
		}
		throw new AssertionError("Expected Kafka command was not observed within 20 seconds");
	}

	private void assertTopicContract() throws Exception {
		try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", "localhost:9092"))) {
			assertThat(admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC).partitions())
					.hasSize(4);
			ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
			Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
			assertThat(config.get("retention.ms")).extracting(ConfigEntry::value)
					.isEqualTo("604800000");
		}
	}
}
