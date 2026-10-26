package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

class PaymentOperationCommandListenerTests {

	@Test
	void admitSubmitsToWorkerPoolAndAcknowledgesOnSuccess() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());
		when(processor.process(eq(record.key()), any())).thenReturn(true);

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		when(admission.currentEpoch(partition)).thenReturn(0L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.ADMIT);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of());
		when(admission.release(any(), eq(partition))).thenReturn(Optional.empty());

		PaymentOperationWorkerPool workerPool = synchronousWorkerPool();

		listener(processor, admission, workerPool).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment).acknowledge();
	}

	@Test
	void admitWithholdsAcknowledgmentWhenEpochChangedDuringProcessing() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());
		when(processor.process(eq(record.key()), any())).thenReturn(true);

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		when(admission.currentEpoch(partition)).thenReturn(0L, 1L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.ADMIT);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of());
		when(admission.release(any(), eq(partition))).thenReturn(Optional.empty());

		PaymentOperationWorkerPool workerPool = synchronousWorkerPool();

		listener(processor, admission, workerPool).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment, never()).acknowledge();
	}

	@Test
	void queuedDoesNotAcknowledgeOrSubmitImmediately() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		when(admission.currentEpoch(partition)).thenReturn(0L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.QUEUED);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of());

		PaymentOperationWorkerPool workerPool = mock(PaymentOperationWorkerPool.class);

		listener(processor, admission, workerPool).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment, never()).acknowledge();
		verifyNoInteractions(workerPool);
	}

	@Test
	void overflowWithholdsAcknowledgmentAndDoesNotSubmitToTheWorkerPool() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		when(admission.currentEpoch(partition)).thenReturn(0L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.OVERFLOW);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of());

		PaymentOperationWorkerPool workerPool = mock(PaymentOperationWorkerPool.class);

		listener(processor, admission, workerPool).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment, never()).acknowledge();
		verifyNoInteractions(workerPool);
	}

	@Test
	void everyInvocationTriggersAResumeCheckOnAdmission() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());
		when(processor.process(eq(record.key()), any())).thenReturn(true);

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		TopicPartition readyPartition = new TopicPartition(record.topic(), 1);
		when(admission.currentEpoch(partition)).thenReturn(0L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.ADMIT);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of(readyPartition));
		when(admission.release(any(), eq(partition))).thenReturn(Optional.empty());

		PaymentOperationWorkerPool workerPool = synchronousWorkerPool();

		listener(processor, admission, workerPool).onPaymentOperationCommand(record, acknowledgment);

		verify(admission, atLeastOnce()).partitionsNeedingResume();
	}

	@Test
	void promotedQueuedCommandIsResubmittedToTheWorkerPoolOnRelease() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		Acknowledgment promotedAcknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());
		ConsumerRecord<String, Object> promotedRecord = consumerRecord(record.key());
		when(processor.process(eq(record.key()), any())).thenReturn(true);

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		UUID paymentId = UUID.fromString(record.key());
		QueuedCommand promoted = new QueuedCommand(promotedRecord, promotedAcknowledgment, partition, paymentId, 0L);
		when(admission.currentEpoch(partition)).thenReturn(0L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.ADMIT);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of());
		when(admission.release(any(), eq(partition))).thenReturn(Optional.of(promoted), Optional.empty());

		PaymentOperationWorkerPool workerPool = synchronousWorkerPool();

		listener(processor, admission, workerPool).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment).acknowledge();
		verify(promotedAcknowledgment).acknowledge();
	}

	@Test
	void workerCompletionResumesPartitionWithoutNewIncomingRecord() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		when(processor.process(any(), any())).thenReturn(true);

		TopicPartition partitionZero = new TopicPartition("payment.operations.requested.v1", 0);
		TopicPartition partitionOne = new TopicPartition("payment.operations.requested.v1", 1);

		KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		when(registry.getListenerContainer(PaymentOperationCommandListener.LISTENER_ID)).thenReturn(container);

		PaymentWorkAdmission admission = new PaymentWorkAdmission(testProperties(2, 1, 4), registry);

		List<Runnable> submittedTasks = new ArrayList<>();
		PaymentOperationWorkerPool workerPool = mock(PaymentOperationWorkerPool.class);
		doAnswer(invocation -> {
			submittedTasks.add(invocation.getArgument(0));
			return null;
		}).when(workerPool).submit(any());

		PaymentOperationCommandListener listener = listener(processor, admission, workerPool);

		listener.onPaymentOperationCommand(
				consumerRecord(partitionZero, UUID.randomUUID().toString()), mock(Acknowledgment.class));
		listener.onPaymentOperationCommand(
				consumerRecord(partitionOne, UUID.randomUUID().toString()), mock(Acknowledgment.class));
		listener.onPaymentOperationCommand(
				consumerRecord(partitionZero, UUID.randomUUID().toString()), mock(Acknowledgment.class));
		listener.onPaymentOperationCommand(
				consumerRecord(partitionOne, UUID.randomUUID().toString()), mock(Acknowledgment.class));

		verify(container).pausePartition(partitionZero);
		verify(container).pausePartition(partitionOne);
		verify(container, never()).resumePartition(any());
		assertThat(submittedTasks).hasSize(2);

		submittedTasks.get(0).run();

		verify(container).resumePartition(partitionZero);
		verify(container, never()).resumePartition(partitionOne);
	}

	@Test
	void overflowRecordIsRetainedAndDispatchedOnceCapacityFreesWithoutNewIncomingRecord() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		when(processor.process(any(), any())).thenReturn(true);

		TopicPartition partitionZero = new TopicPartition("payment.operations.requested.v1", 0);

		KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		when(registry.getListenerContainer(PaymentOperationCommandListener.LISTENER_ID)).thenReturn(container);

		PaymentWorkAdmission admission = new PaymentWorkAdmission(testProperties(1, 1, 4), registry);

		List<Runnable> submittedTasks = new ArrayList<>();
		PaymentOperationWorkerPool workerPool = mock(PaymentOperationWorkerPool.class);
		doAnswer(invocation -> {
			submittedTasks.add(invocation.getArgument(0));
			return null;
		}).when(workerPool).submit(any());

		PaymentOperationCommandListener listener = listener(processor, admission, workerPool);

		Acknowledgment firstAcknowledgment = mock(Acknowledgment.class);
		Acknowledgment overflowAcknowledgment = mock(Acknowledgment.class);

		listener.onPaymentOperationCommand(
				consumerRecord(partitionZero, UUID.randomUUID().toString()), firstAcknowledgment);
		listener.onPaymentOperationCommand(
				consumerRecord(partitionZero, UUID.randomUUID().toString()), overflowAcknowledgment);

		verify(container).pausePartition(partitionZero);
		assertThat(submittedTasks).hasSize(1);

		submittedTasks.get(0).run();

		verify(container).resumePartition(partitionZero);
		assertThat(submittedTasks).hasSize(2);

		submittedTasks.get(1).run();

		verify(overflowAcknowledgment).acknowledge();
	}

	@Test
	void ineligibleAcknowledgmentIsRecordedAndEligibleAcknowledgmentClearsIt() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		ConsumerRecord<String, Object> record = consumerRecord(UUID.randomUUID().toString());
		when(processor.process(eq(record.key()), any())).thenReturn(false);

		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		when(admission.currentEpoch(partition)).thenReturn(0L);
		when(admission.tryAdmit(any(), eq(partition), any())).thenReturn(PaymentWorkAdmission.AdmissionResult.ADMIT);
		when(admission.partitionsNeedingResume()).thenReturn(Set.of());
		when(admission.release(any(), eq(partition))).thenReturn(Optional.empty());

		BlockedPartitionDiagnostics diagnostics = mock(BlockedPartitionDiagnostics.class);
		PaymentOperationWorkerPool workerPool = synchronousWorkerPool();

		new PaymentOperationCommandListener(processor, admission, workerPool, diagnostics)
				.onPaymentOperationCommand(record, mock(Acknowledgment.class));

		verify(diagnostics).recordIneligible(eq(partition), any());
		verify(diagnostics, never()).recordEligible(any());
	}

	private ProcessorProperties testProperties(int globalCapacity, int perPartitionCapacity, int perPaymentWaitingCapacity) {
		return new ProcessorProperties(
				URI.create("http://localhost:8081"),
				Duration.ofSeconds(1),
				List.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20)),
				Duration.ofSeconds(3),
				Duration.ofSeconds(120),
				Duration.ofMillis(1),
				Duration.ofMillis(1),
				new ProcessorProperties.Reconciliation(
						Duration.ofSeconds(5), 20, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(60)),
				new ProcessorProperties.Workers(globalCapacity, perPartitionCapacity, perPaymentWaitingCapacity, 4));
	}

	private PaymentOperationCommandListener listener(
			PaymentOperationProcessor processor, PaymentWorkAdmission admission, PaymentOperationWorkerPool workerPool) {
		return new PaymentOperationCommandListener(processor, admission, workerPool, mock(BlockedPartitionDiagnostics.class));
	}

	private PaymentOperationWorkerPool synchronousWorkerPool() {
		PaymentOperationWorkerPool workerPool = mock(PaymentOperationWorkerPool.class);
		doAnswer(invocation -> {
			Runnable task = invocation.getArgument(0);
			task.run();
			return null;
		}).when(workerPool).submit(any());
		return workerPool;
	}

	private ConsumerRecord<String, Object> consumerRecord(String key) {
		GenericRecord value = mock(GenericRecord.class);
		return new ConsumerRecord<>("payment.operations.requested.v1", 0, 0L, key, value);
	}

	private ConsumerRecord<String, Object> consumerRecord(TopicPartition partition, String key) {
		GenericRecord value = mock(GenericRecord.class);
		return new ConsumerRecord<>(partition.topic(), partition.partition(), 0L, key, value);
	}
}
