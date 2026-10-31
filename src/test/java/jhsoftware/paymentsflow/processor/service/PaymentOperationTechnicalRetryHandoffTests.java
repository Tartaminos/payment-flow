package jhsoftware.paymentsflow.processor.service;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationQuarantineEnvelopeDto;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationCommandDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationQuarantinePublisher;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRawValueDecoder;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryIdentity;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryPublisher;
import jhsoftware.paymentsflow.processor.repository.PaymentOperationQuarantineRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
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
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;

class PaymentOperationTechnicalRetryHandoffTests {

	private static final String TOPIC = "payment.operations.requested.v1";
	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

	private final PaymentOperationRawValueDecoder rawValueDecoder = mock(PaymentOperationRawValueDecoder.class);
	private final PaymentOperationCommandDecoder commandDecoder = mock(PaymentOperationCommandDecoder.class);
	private final PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
	private final TechnicalFailureClassifier classifier = mock(TechnicalFailureClassifier.class);
	private final PaymentOperationRetryPublisher retryPublisher = mock(PaymentOperationRetryPublisher.class);
	private final PaymentOperationQuarantineRepository quarantineRepository = mock(PaymentOperationQuarantineRepository.class);
	private final PaymentOperationQuarantinePublisher quarantinePublisher = mock(PaymentOperationQuarantinePublisher.class);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
	private final PaymentOperationQuarantineHandoff quarantineHandoff =
			new PaymentOperationQuarantineHandoff(quarantineRepository, quarantinePublisher, clock);

	private final PaymentOperationTechnicalRetryHandoff handoff = new PaymentOperationTechnicalRetryHandoff(
			rawValueDecoder, commandDecoder, processor, classifier, retryPublisher,
			quarantineHandoff, clock);

	@Test
	void successfulProcessingReturnsItsResultWithoutClassifyingOrPublishing() {
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 0, 5L);
		GenericRecord decoded = mock(GenericRecord.class);
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(any(), org.mockito.ArgumentMatchers.same(decoded))).thenReturn(true);

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
		ConsumerRecord<byte[], Object> record = record(key, 3, 42L);
		GenericRecord decoded = mock(GenericRecord.class);
		DataAccessResourceFailureException failure = new DataAccessResourceFailureException("db down");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(key, decoded)).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.of("DATA_ACCESS_UNAVAILABLE"));
		when(commandDecoder.decode(decoded)).thenReturn(
				new PaymentOperationCommand(messageId, "ExecutePaymentOperation", operationId, correlationId, null, NOW));
		when(retryPublisher.publish(any())).thenReturn(true);

		boolean ackEligible = handoff.handle(record);

		assertThat(ackEligible).isTrue();
		ArgumentCaptor<PaymentOperationRetryEnvelopeDto> captor = ArgumentCaptor.forClass(PaymentOperationRetryEnvelopeDto.class);
		verify(retryPublisher).publish(captor.capture());
		PaymentOperationRetryEnvelopeDto envelope = captor.getValue();
		assertThat(envelope.sourceTopic()).isEqualTo(TOPIC);
		assertThat(envelope.sourcePartition()).isEqualTo(3);
		assertThat(envelope.sourceOffset()).isEqualTo(42L);
		assertThat(envelope.sourceRecordKey()).isEqualTo(record.key());
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
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 0, 5L);
		GenericRecord decoded = mock(GenericRecord.class);
		DataAccessResourceFailureException failure = new DataAccessResourceFailureException("db down");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(any(), org.mockito.ArgumentMatchers.same(decoded))).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.of("DATA_ACCESS_UNAVAILABLE"));
		when(commandDecoder.decode(decoded)).thenThrow(new IllegalStateException("cannot decode"));
		when(retryPublisher.publish(any())).thenReturn(false);

		assertThatThrownBy(() -> handoff.handle(record)).isSameAs(failure);
	}

	@Test
	void unclassifiedFailurePropagatesWithoutPublishing() {
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 0, 5L);
		GenericRecord decoded = mock(GenericRecord.class);
		IllegalStateException failure = new IllegalStateException("poison");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(any(), org.mockito.ArgumentMatchers.same(decoded))).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.empty());

		assertThatThrownBy(() -> handoff.handle(record)).isSameAs(failure);
		verify(retryPublisher, never()).publish(any());
	}

	@Test
	void redeliveredSourceCoordinatesProduceIdenticalRetryId() {
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 3, 42L);
		GenericRecord decoded = mock(GenericRecord.class);
		DataAccessResourceFailureException failure = new DataAccessResourceFailureException("db down");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(any(), org.mockito.ArgumentMatchers.same(decoded))).thenThrow(failure);
		when(classifier.classifyTransient(failure)).thenReturn(java.util.Optional.of("DATA_ACCESS_UNAVAILABLE"));
		when(commandDecoder.decode(decoded)).thenThrow(new IllegalStateException("cannot decode"));
		when(retryPublisher.publish(any())).thenReturn(true);

		handoff.handle(record);
		handoff.handle(record);

		ArgumentCaptor<PaymentOperationRetryEnvelopeDto> captor = ArgumentCaptor.forClass(PaymentOperationRetryEnvelopeDto.class);
		verify(retryPublisher, org.mockito.Mockito.times(2)).publish(captor.capture());
		assertThat(captor.getAllValues()).extracting(PaymentOperationRetryEnvelopeDto::retryId)
				.containsExactly(captor.getAllValues().get(0).retryId(), captor.getAllValues().get(0).retryId());
	}

	@Test
	void poisonCommandIsPersistedBeforeDlqAcknowledgmentMakesSourceEligible() {
		ConsumerRecord<byte[], Object> record = record("not-a-payment-id", 1, 17L);
		PoisonCommandException failure = new PoisonCommandException(PoisonFailureCode.CORRUPTED_WIRE_FORMAT);
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenThrow(failure);
		when(quarantinePublisher.publish(any())).thenReturn(true);

		boolean ackEligible = handoff.handle(record);

		assertThat(ackEligible).isTrue();
		ArgumentCaptor<PaymentOperationQuarantineEnvelopeDto> captor =
				ArgumentCaptor.forClass(PaymentOperationQuarantineEnvelopeDto.class);
		InOrder order = inOrder(quarantineRepository, quarantinePublisher);
		order.verify(quarantineRepository).persist(captor.capture());
		order.verify(quarantinePublisher).publish(captor.getValue());
		PaymentOperationQuarantineEnvelopeDto envelope = captor.getValue();
		assertThat(envelope.quarantineId())
				.isEqualTo(PaymentOperationRetryIdentity.quarantineId(TOPIC, 1, 17L));
		assertThat(envelope.failureCode()).isEqualTo("CORRUPTED_WIRE_FORMAT");
		assertThat(envelope.sourceRecordValue()).isEqualTo(record.value());
		verify(retryPublisher, never()).publish(any());
	}

	@Test
	void failedDlqPublicationLeavesPoisonSourceIneligible() {
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 1, 17L);
		PoisonCommandException failure = new PoisonCommandException(PoisonFailureCode.INVALID_AVRO_PAYLOAD);
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenThrow(failure);
		when(quarantinePublisher.publish(any())).thenReturn(false);

		assertThatThrownBy(() -> handoff.handle(record)).isSameAs(failure);
		verify(quarantineRepository).persist(any());
	}

	@Test
	void quarantinePersistenceFailureNeverPublishesOrMakesSourceEligible() {
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 1, 17L);
		PoisonCommandException poison = new PoisonCommandException(PoisonFailureCode.INVALID_AVRO_PAYLOAD);
		IllegalStateException databaseFailure = new IllegalStateException("database unavailable");
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenThrow(poison);
		org.mockito.Mockito.doThrow(databaseFailure).when(quarantineRepository).persist(any());

		assertThatThrownBy(() -> handoff.handle(record)).isSameAs(databaseFailure);
		verify(quarantinePublisher, never()).publish(any());
	}

	@Test
	void unsupportedCommandTypeRetainsDecodableDiagnosticIdentities() {
		UUID operationId = UUID.fromString("22222222-2222-4222-8222-222222222222");
		UUID correlationId = UUID.fromString("33333333-3333-4333-8333-333333333333");
		UUID messageId = UUID.fromString("44444444-4444-4444-8444-444444444444");
		ConsumerRecord<byte[], Object> record = record(UUID.randomUUID().toString(), 2, 18L);
		GenericRecord decoded = mock(GenericRecord.class);
		PoisonCommandException failure = new PoisonCommandException(PoisonFailureCode.UNSUPPORTED_COMMAND_TYPE);
		when(rawValueDecoder.decode(TOPIC, (byte[]) record.value())).thenReturn(decoded);
		when(processor.process(any(), org.mockito.ArgumentMatchers.same(decoded))).thenThrow(failure);
		when(commandDecoder.decodeForDiagnostics(decoded)).thenReturn(new PaymentOperationCommand(
				messageId, "UnsupportedCommand", operationId, correlationId, null, NOW));
		when(quarantinePublisher.publish(any())).thenReturn(true);

		assertThat(handoff.handle(record)).isTrue();

		ArgumentCaptor<PaymentOperationQuarantineEnvelopeDto> captor =
				ArgumentCaptor.forClass(PaymentOperationQuarantineEnvelopeDto.class);
		verify(quarantineRepository).persist(captor.capture());
		assertThat(captor.getValue().operationId()).isEqualTo(operationId);
		assertThat(captor.getValue().correlationId()).isEqualTo(correlationId);
		assertThat(captor.getValue().sourceMessageId()).isEqualTo(messageId);
	}

	@Test
	void nullKafkaValueIsQuarantinedAsEmptyRequiredEvidenceBytes() {
		byte[] key = UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
		ConsumerRecord<byte[], Object> record = new ConsumerRecord<>(TOPIC, 2, 19L, key, null);
		PoisonCommandException failure = new PoisonCommandException(PoisonFailureCode.CORRUPTED_WIRE_FORMAT);
		when(rawValueDecoder.decode(org.mockito.ArgumentMatchers.eq(TOPIC), any(byte[].class))).thenThrow(failure);
		when(quarantinePublisher.publish(any())).thenReturn(true);

		assertThat(handoff.handle(record)).isTrue();

		ArgumentCaptor<PaymentOperationQuarantineEnvelopeDto> captor =
				ArgumentCaptor.forClass(PaymentOperationQuarantineEnvelopeDto.class);
		verify(quarantineRepository).persist(captor.capture());
		assertThat(captor.getValue().sourceRecordValue()).isEmpty();
		assertThat(captor.getValue().failureCode()).isEqualTo("CORRUPTED_WIRE_FORMAT");
	}

	private ConsumerRecord<byte[], Object> record(String key, int partition, long offset) {
		return new ConsumerRecord<>(TOPIC, partition, offset,
				key.getBytes(java.nio.charset.StandardCharsets.UTF_8), new byte[] {1, 2, 3, 4});
	}
}
