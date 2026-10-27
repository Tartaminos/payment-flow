package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;

class PaymentOperationTechnicalRetryHandoffTests {

	private static final String TOPIC = "payment.operations.requested.v1";
	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

	private final PaymentOperationRawValueDecoder rawValueDecoder = mock(PaymentOperationRawValueDecoder.class);
	private final PaymentOperationCommandDecoder commandDecoder = mock(PaymentOperationCommandDecoder.class);
	private final PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
	private final TechnicalFailureClassifier classifier = mock(TechnicalFailureClassifier.class);
	private final PaymentOperationRetryPublisher retryPublisher = mock(PaymentOperationRetryPublisher.class);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

	private final PaymentOperationTechnicalRetryHandoff handoff = new PaymentOperationTechnicalRetryHandoff(
			rawValueDecoder, commandDecoder, processor, classifier, retryPublisher, clock);

	@Test
	void successfulProcessingReturnsItsResultWithoutClassifyingOrPublishing() {
		ConsumerRecord<String, Object> record = record(UUID.randomUUID().toString(), 0, 5L);
		GenericRecord decoded = mock(GenericRecord.class);
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(record.key(), decoded)).thenReturn(true);

		boolean ackEligible = handoff.handle(record);

		assertThat(ackEligible).isTrue();
		verify(classifier, never()).classifyTransient(any());
		verify(retryPublisher, never()).publish(any());
	}

	@Test
	void transientFailureWithSuccessfulPublishIsAckEligibleAndCarriesSourceCoordinates() {
		UUID operationId = UUID.fromString("22222222-2222-4222-8222-222222222222");
		UUID correlationId = UUID.fromString("33333333-3333-4333-8333-333333333333");
		UUID messageId = UUID.fromString("44444444-4444-4444-8444-444444444444");
		String key = UUID.randomUUID().toString();
		ConsumerRecord<String, Object> record = record(key, 3, 42L);
		GenericRecord decoded = mock(GenericRecord.class);
		DataAccessResourceFailureException failure = new DataAccessResourceFailureException("db down");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(record.key(), decoded)).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.of("DATA_ACCESS_UNAVAILABLE"));
		when(commandDecoder.decode(decoded)).thenReturn(
				new PaymentOperationCommand(messageId, "ExecutePaymentOperation", operationId, correlationId, null, NOW));
		when(retryPublisher.publish(any())).thenReturn(true);

		boolean ackEligible = handoff.handle(record);

		assertThat(ackEligible).isTrue();
		ArgumentCaptor<PaymentOperationRetryEnvelope> captor = ArgumentCaptor.forClass(PaymentOperationRetryEnvelope.class);
		verify(retryPublisher).publish(captor.capture());
		PaymentOperationRetryEnvelope envelope = captor.getValue();
		assertThat(envelope.sourceTopic()).isEqualTo(TOPIC);
		assertThat(envelope.sourcePartition()).isEqualTo(3);
		assertThat(envelope.sourceOffset()).isEqualTo(42L);
		assertThat(envelope.sourceRecordKey()).isEqualTo(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
		assertThat(envelope.sourceRecordValue()).isEqualTo(record.value());
		assertThat(envelope.failureClass()).isEqualTo("DATA_ACCESS_UNAVAILABLE");
		assertThat(envelope.failedAt()).isEqualTo(NOW);
		assertThat(envelope.operationId()).isEqualTo(operationId);
		assertThat(envelope.correlationId()).isEqualTo(correlationId);
		assertThat(envelope.sourceMessageId()).isEqualTo(messageId);
		assertThat(envelope.retryId()).isEqualTo(PaymentOperationRetryIdentity.retryId(TOPIC, 3, 42L));
		verify(rawValueDecoder).decode(TOPIC, (byte[]) record.value());
	}

	@Test
	void transientFailureWithFailedPublishRethrowsOriginalFailure() {
		ConsumerRecord<String, Object> record = record(UUID.randomUUID().toString(), 0, 5L);
		GenericRecord decoded = mock(GenericRecord.class);
		DataAccessResourceFailureException failure = new DataAccessResourceFailureException("db down");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(record.key(), decoded)).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.of("DATA_ACCESS_UNAVAILABLE"));
		when(commandDecoder.decode(decoded)).thenThrow(new IllegalStateException("cannot decode"));
		when(retryPublisher.publish(any())).thenReturn(false);

		assertThatThrownBy(() -> handoff.handle(record)).isSameAs(failure);
	}

	@Test
	void unclassifiedFailurePropagatesWithoutPublishing() {
		ConsumerRecord<String, Object> record = record(UUID.randomUUID().toString(), 0, 5L);
		GenericRecord decoded = mock(GenericRecord.class);
		IllegalStateException failure = new IllegalStateException("poison");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(record.key(), decoded)).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.empty());

		assertThatThrownBy(() -> handoff.handle(record)).isSameAs(failure);
		verify(retryPublisher, never()).publish(any());
	}

	@Test
	void redeliveredSourceCoordinatesProduceIdenticalRetryId() {
		ConsumerRecord<String, Object> record = record(UUID.randomUUID().toString(), 3, 42L);
		GenericRecord decoded = mock(GenericRecord.class);
		DataAccessResourceFailureException failure = new DataAccessResourceFailureException("db down");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(record.key(), decoded)).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.of("DATA_ACCESS_UNAVAILABLE"));
		when(commandDecoder.decode(decoded)).thenThrow(new IllegalStateException("cannot decode"));
		when(retryPublisher.publish(any())).thenReturn(true);

		handoff.handle(record);
		handoff.handle(record);

		ArgumentCaptor<PaymentOperationRetryEnvelope> captor = ArgumentCaptor.forClass(PaymentOperationRetryEnvelope.class);
		verify(retryPublisher, org.mockito.Mockito.times(2)).publish(captor.capture());
		assertThat(captor.getAllValues()).extracting(PaymentOperationRetryEnvelope::retryId)
				.containsExactly(captor.getAllValues().get(0).retryId(), captor.getAllValues().get(0).retryId());
	}

	private ConsumerRecord<String, Object> record(String key, int partition, long offset) {
		return new ConsumerRecord<>(TOPIC, partition, offset, key, new byte[] {1, 2, 3, 4});
	}
}
