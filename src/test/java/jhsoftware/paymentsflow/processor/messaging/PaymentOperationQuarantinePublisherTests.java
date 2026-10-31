package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.PaymentOperationQuarantineMessagingConfiguration;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationQuarantineEnvelopeDto;

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

class PaymentOperationQuarantinePublisherTests {

	@Test
	void acknowledgedDlqSendContainsRecoverableEvidence() {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
		Schema schema = new PaymentOperationQuarantineMessagingConfiguration().paymentOperationQuarantineSchema();
		PaymentOperationQuarantinePublisher publisher = new PaymentOperationQuarantinePublisher(
				kafkaTemplate, schema,
				new PaymentOperationQuarantineMessagingConfiguration.SchemaRegistryContract(1));
		PaymentOperationQuarantineEnvelopeDto envelope = envelope();
		when(kafkaTemplate.send(eq(PaymentOperationQuarantineMessagingConfiguration.TOPIC),
				eq(envelope.quarantineId().toString()), any()))
				.thenReturn(CompletableFuture.completedFuture(null));

		assertThat(publisher.publish(envelope)).isTrue();

		ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
		verify(kafkaTemplate).send(eq(PaymentOperationQuarantineMessagingConfiguration.TOPIC),
				eq(envelope.quarantineId().toString()), value.capture());
		GenericRecord record = (GenericRecord) value.getValue();
		assertThat(record.get("failureCode").toString()).isEqualTo("INVALID_AVRO_PAYLOAD");
		assertThat(((ByteBuffer) record.get("sourceRecordKey")).array()).isEqualTo(new byte[] {1, 2});
		assertThat(((ByteBuffer) record.get("sourceRecordValue")).array()).isEqualTo(new byte[] {3, 4});
	}

	@Test
	void dlqContractUsesOnePartitionAndSevenDayRetention() {
		org.apache.kafka.clients.admin.NewTopic topic =
				new PaymentOperationQuarantineMessagingConfiguration().paymentOperationsDlqTopic();

		assertThat(topic.name()).isEqualTo("payment.operations.dlq.v1");
		assertThat(topic.numPartitions()).isEqualTo(1);
		assertThat(topic.replicationFactor()).isEqualTo((short) 1);
		assertThat(topic.configs()).containsEntry(
				org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG, "604800000");
	}

	private PaymentOperationQuarantineEnvelopeDto envelope() {
		return new PaymentOperationQuarantineEnvelopeDto(
				UUID.fromString("11111111-1111-5111-9111-111111111111"),
				"payment.operations.requested.v1", 2, 17L,
				new byte[] {1, 2}, new byte[] {3, 4}, "INVALID_AVRO_PAYLOAD",
				Instant.parse("2026-10-27T10:00:00Z"), null, null, null);
	}
}
