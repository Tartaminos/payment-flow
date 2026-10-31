package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.PaymentOperationRetryMessagingConfiguration;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class PaymentOperationRetryPublisherTests {

	private static final UUID RETRY_ID = UUID.fromString("11111111-1111-5111-9111-111111111111");
	private static final UUID OPERATION_ID = UUID.fromString("22222222-2222-4222-8222-222222222222");
	private static final UUID CORRELATION_ID = UUID.fromString("33333333-3333-4333-8333-333333333333");
	private static final UUID SOURCE_MESSAGE_ID = UUID.fromString("44444444-4444-4444-8444-444444444444");
	private static final Instant FAILED_AT = Instant.parse("2026-10-27T10:00:00Z");

	@Test
	void acknowledgedSendBuildsEnvelopeRecordAndReturnsTrue() throws Exception {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		Schema schema = new PaymentOperationRetryMessagingConfiguration().paymentOperationRetrySchema();
		PaymentOperationRetryPublisher publisher = new PaymentOperationRetryPublisher(
				kafkaTemplate, schema, new PaymentOperationRetryMessagingConfiguration.SchemaRegistryContract(1));
		when(kafkaTemplate.send(eq(PaymentOperationRetryMessagingConfiguration.TOPIC), eq(RETRY_ID.toString()), any()))
				.thenReturn(CompletableFuture.completedFuture(null));

		PaymentOperationRetryEnvelopeDto envelope = new PaymentOperationRetryEnvelopeDto(
				RETRY_ID,
				"payment.operations.requested.v1",
				2,
				17L,
				"paymentId".getBytes(),
				new byte[] {1, 2, 3},
				"DATA_ACCESS_UNAVAILABLE",
				FAILED_AT,
				OPERATION_ID,
				CORRELATION_ID,
				SOURCE_MESSAGE_ID);

		boolean published = publisher.publish(envelope);

		assertThat(published).isTrue();
		ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
		verify(kafkaTemplate).send(eq(PaymentOperationRetryMessagingConfiguration.TOPIC), eq(RETRY_ID.toString()), value.capture());
		GenericRecord record = (GenericRecord) value.getValue();
		assertThat(record.get("retryId").toString()).isEqualTo(RETRY_ID.toString());
		assertThat(record.get("sourceTopic").toString()).isEqualTo("payment.operations.requested.v1");
		assertThat(record.get("sourcePartition")).isEqualTo(2);
		assertThat(record.get("sourceOffset")).isEqualTo(17L);
		assertThat(((ByteBuffer) record.get("sourceRecordKey")).array()).isEqualTo("paymentId".getBytes());
		assertThat(((ByteBuffer) record.get("sourceRecordValue")).array()).isEqualTo(new byte[] {1, 2, 3});
		assertThat(record.get("failureClass").toString()).isEqualTo("DATA_ACCESS_UNAVAILABLE");
		assertThat(record.get("failedAt")).isEqualTo(FAILED_AT.toEpochMilli());
		assertThat(record.get("operationId").toString()).isEqualTo(OPERATION_ID.toString());
		assertThat(record.get("correlationId").toString()).isEqualTo(CORRELATION_ID.toString());
		assertThat(record.get("sourceMessageId").toString()).isEqualTo(SOURCE_MESSAGE_ID.toString());
	}

	@Test
	void nullOptionalFieldsArePublishedAsNull() throws Exception {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		Schema schema = new PaymentOperationRetryMessagingConfiguration().paymentOperationRetrySchema();
		PaymentOperationRetryPublisher publisher = new PaymentOperationRetryPublisher(
				kafkaTemplate, schema, new PaymentOperationRetryMessagingConfiguration.SchemaRegistryContract(1));
		when(kafkaTemplate.send(eq(PaymentOperationRetryMessagingConfiguration.TOPIC), eq(RETRY_ID.toString()), any()))
				.thenReturn(CompletableFuture.completedFuture(null));

		PaymentOperationRetryEnvelopeDto envelope = new PaymentOperationRetryEnvelopeDto(
				RETRY_ID,
				"payment.operations.requested.v1",
				2,
				17L,
				new byte[0],
				new byte[] {9},
				"DATA_ACCESS_UNAVAILABLE",
				FAILED_AT,
				null,
				null,
				null);

		publisher.publish(envelope);

		ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
		verify(kafkaTemplate).send(eq(PaymentOperationRetryMessagingConfiguration.TOPIC), eq(RETRY_ID.toString()), value.capture());
		GenericRecord record = (GenericRecord) value.getValue();
		assertThat(record.get("operationId")).isNull();
		assertThat(record.get("correlationId")).isNull();
		assertThat(record.get("sourceMessageId")).isNull();
	}

	@Test
	void failedSendReturnsFalseWithoutThrowing() {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		Schema schema = new PaymentOperationRetryMessagingConfiguration().paymentOperationRetrySchema();
		PaymentOperationRetryPublisher publisher = new PaymentOperationRetryPublisher(
				kafkaTemplate, schema, new PaymentOperationRetryMessagingConfiguration.SchemaRegistryContract(1));
		when(kafkaTemplate.send(eq(PaymentOperationRetryMessagingConfiguration.TOPIC), eq(RETRY_ID.toString()), any()))
				.thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker unavailable")));

		PaymentOperationRetryEnvelopeDto envelope = new PaymentOperationRetryEnvelopeDto(
				RETRY_ID,
				"payment.operations.requested.v1",
				2,
				17L,
				new byte[0],
				new byte[] {9},
				"DATA_ACCESS_UNAVAILABLE",
				FAILED_AT,
				null,
				null,
				null);

		boolean published = publisher.publish(envelope);

		assertThat(published).isFalse();
	}

	@Test
	void topicContractUsesTwoPartitionsAndSevenDayRetention() {
		org.apache.kafka.clients.admin.NewTopic topic =
				new PaymentOperationRetryMessagingConfiguration().paymentOperationsRetryTopic();

		assertThat(topic.name()).isEqualTo("payment.operations.retry.v1");
		assertThat(topic.numPartitions()).isEqualTo(2);
		assertThat(topic.replicationFactor()).isEqualTo((short) 1);
		assertThat(topic.configs()).containsEntry(
				org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG, "604800000");
	}
}
