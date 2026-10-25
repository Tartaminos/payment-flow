package jhsoftware.paymentsflow.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class OutboxPublisherTests {

	private static final UUID MESSAGE_ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
	private static final UUID PAYMENT_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
	private static final UUID OPERATION_ID = UUID.fromString("22222222-2222-4222-8222-222222222222");
	private static final UUID CORRELATION_ID = UUID.fromString("33333333-3333-4333-8333-333333333333");
	private static final Instant OCCURRED_AT = Instant.parse("2026-10-24T18:30:45.123Z");
	private static final Instant PUBLISHED_AT = Instant.parse("2026-10-25T10:00:00Z");

	@Test
	void acknowledgedSendUsesPersistedCanonicalKeyAndEnvelopeThenMarksPublished() throws Exception {
		OutboxPublicationRepository repository = mock(OutboxPublicationRepository.class);
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		Schema schema = new OutboxMessagingConfiguration().executePaymentOperationSchema();
		OutboxPublication publication = publication(PAYMENT_ID);
		when(repository.findPending(20)).thenReturn(List.of(publication));
		when(kafkaTemplate.send(eq(OutboxMessagingConfiguration.TOPIC), eq(PAYMENT_ID.toString()),
				org.mockito.ArgumentMatchers.any()))
				.thenReturn(CompletableFuture.completedFuture(null));
		when(repository.markPublished(MESSAGE_ID, PUBLISHED_AT)).thenReturn(true);

		publisher(repository, kafkaTemplate, schema).publishPending();

		ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
		verify(kafkaTemplate).send(eq(OutboxMessagingConfiguration.TOPIC), key.capture(), value.capture());
		assertThat(key.getValue())
				.isEqualTo("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
				.hasSize(36)
				.doesNotContainPattern("[A-F]");

		GenericRecord record = (GenericRecord) value.getValue();
		assertThat(record.getSchema().getFields()).extracting(Schema.Field::name)
				.containsExactly("messageId", "commandType", "operationId", "correlationId", "occurredAt", "causationId");
		assertThat(record.get("messageId").toString()).isEqualTo(MESSAGE_ID.toString());
		assertThat(record.get("commandType").toString()).isEqualTo("ExecutePaymentOperation");
		assertThat(record.get("operationId").toString()).isEqualTo(OPERATION_ID.toString());
		assertThat(record.get("correlationId").toString()).isEqualTo(CORRELATION_ID.toString());
		assertThat(record.get("occurredAt")).isEqualTo(OCCURRED_AT.toEpochMilli());
		assertThat(record.get("causationId")).isNull();
		verify(repository).markPublished(MESSAGE_ID, PUBLISHED_AT);
	}

	@Test
	void failedSendLeavesPendingStateUntouched() {
		OutboxPublicationRepository repository = mock(OutboxPublicationRepository.class);
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		when(repository.findPending(20)).thenReturn(List.of(publication(PAYMENT_ID)));
		when(kafkaTemplate.send(eq(OutboxMessagingConfiguration.TOPIC), eq(PAYMENT_ID.toString()),
				org.mockito.ArgumentMatchers.any()))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker unavailable")));

		publisher(repository, kafkaTemplate, new OutboxMessagingConfiguration().executePaymentOperationSchema())
				.publishPending();

		verify(repository, never()).markPublished(org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.any());
	}

	@Test
	void inconsistentAuthoritativePaymentIdentityIsNotPublished() {
		OutboxPublicationRepository repository = mock(OutboxPublicationRepository.class);
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		when(repository.findPending(20)).thenReturn(List.of(
				publication(UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"))));

		publisher(repository, kafkaTemplate, new OutboxMessagingConfiguration().executePaymentOperationSchema())
				.publishPending();

		verify(kafkaTemplate, never()).send(
				org.mockito.ArgumentMatchers.anyString(),
				org.mockito.ArgumentMatchers.anyString(),
				org.mockito.ArgumentMatchers.any());
		verify(repository, never()).markPublished(org.mockito.ArgumentMatchers.any(),
				org.mockito.ArgumentMatchers.any());
	}

	@Test
	void schemaRegistryContractProducesRecordNameSubjectAndDecodableConfluentWireValue() throws Exception {
		OutboxMessagingConfiguration configuration = new OutboxMessagingConfiguration();
		Schema schema = configuration.executePaymentOperationSchema();
		MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
		OutboxMessagingConfiguration.SchemaRegistryContract contract =
				configuration.executePaymentOperationSchemaRegistration(registry, schema);

		GenericRecord record = new GenericData.Record(schema);
		record.put("messageId", MESSAGE_ID.toString());
		record.put("commandType", "ExecutePaymentOperation");
		record.put("operationId", OPERATION_ID.toString());
		record.put("correlationId", CORRELATION_ID.toString());
		record.put("occurredAt", OCCURRED_AT.toEpochMilli());
		record.put("causationId", null);
		Map<String, Object> serializerConfig = Map.of(
				"schema.registry.url", "mock://m2-contract",
				"auto.register.schemas", false,
				"use.latest.version", false,
				"value.subject.name.strategy", RecordNameStrategy.class.getName());

		byte[] bytes;
		try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(registry, serializerConfig)) {
			bytes = serializer.serialize(OutboxMessagingConfiguration.TOPIC, record);
		}

		assertThat(bytes[0]).isZero();
		assertThat(ByteBuffer.wrap(bytes, 1, Integer.BYTES).getInt()).isEqualTo(contract.schemaId());
		assertThat(registry.getAllSubjects()).containsExactly(OutboxMessagingConfiguration.SUBJECT);
		assertThat(registry.getCompatibility(OutboxMessagingConfiguration.SUBJECT))
				.isEqualTo(OutboxMessagingConfiguration.COMPATIBILITY);

		try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(registry)) {
			GenericRecord decoded = (GenericRecord) deserializer.deserialize(
					OutboxMessagingConfiguration.TOPIC, bytes);
			assertThat(decoded.get("messageId").toString()).isEqualTo(MESSAGE_ID.toString());
			assertThat(decoded.get("operationId").toString()).isEqualTo(OPERATION_ID.toString());
			assertThat(decoded.get("correlationId").toString()).isEqualTo(CORRELATION_ID.toString());
			assertThat(decoded.get("causationId")).isNull();
			assertThat(decoded.getSchema().getField("paymentId")).isNull();
			assertThat(decoded.getSchema().getField("schemaVersion")).isNull();
		}

		Schema incompatible = new Schema.Parser().parse(schema.toString().replace(
				"{\"name\":\"messageId\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}",
				"{\"name\":\"messageId\",\"type\":\"long\"}"));
		assertThat(registry.testCompatibility(
				OutboxMessagingConfiguration.SUBJECT, new AvroSchema(incompatible))).isFalse();
	}

	@Test
	void topicContractUsesFourPartitionsAndSevenDayRetention() {
		NewTopic topic = new OutboxMessagingConfiguration().paymentOperationsRequestedTopic();

		assertThat(topic.name()).isEqualTo("payment.operations.requested.v1");
		assertThat(topic.numPartitions()).isEqualTo(4);
		assertThat(topic.replicationFactor()).isEqualTo((short) 1);
		assertThat(topic.configs()).containsEntry(TopicConfig.RETENTION_MS_CONFIG, "604800000");
	}

	private OutboxPublisher publisher(
			OutboxPublicationRepository repository,
			KafkaTemplate<String, Object> kafkaTemplate,
			Schema schema) {
		return new OutboxPublisher(
				repository,
				kafkaTemplate,
				schema,
				new OutboxMessagingConfiguration.SchemaRegistryContract(1),
				new OutboxMessagingProperties(
						URI.create("http://localhost:8081"),
						new OutboxMessagingProperties.Publisher(20, Duration.ofSeconds(1))),
				Clock.fixed(PUBLISHED_AT, ZoneOffset.UTC));
	}

	private OutboxPublication publication(UUID authoritativePaymentId) {
		return new OutboxPublication(
				MESSAGE_ID,
				PAYMENT_ID,
				OPERATION_ID,
				authoritativePaymentId,
				"ExecutePaymentOperation",
				CORRELATION_ID,
				null,
				OCCURRED_AT);
	}
}
