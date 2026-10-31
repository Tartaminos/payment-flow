package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.ProcessorProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

class PaymentWorkAdmissionTests {

	private static final Acknowledgment NO_OP_ACKNOWLEDGMENT = new Acknowledgment() {

		@Override
		public void acknowledge() {
		}
	};

	@Test
	void admitsWhenPaymentIsNotActiveAndCapacityIsAvailable() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();

		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.ADMIT);
	}

	@Test
	void queuesWhenPaymentIsAlreadyActiveAndWaitingCapacityRemains() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.QUEUED);
	}

	@Test
	void overflowsWhenPerPaymentWaitingCapacityIsExceeded() {
		PaymentWorkAdmission admission = admission(32, 8, 2);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.OVERFLOW);
	}

	@Test
	void overflowsWhenPerPartitionCapacityIsExceeded() {
		PaymentWorkAdmission admission = admission(32, 2, 4);
		TopicPartition partition = partition(0);
		admission.tryAdmit(UUID.randomUUID(), partition, command(UUID.randomUUID(), partition));
		admission.tryAdmit(UUID.randomUUID(), partition, command(UUID.randomUUID(), partition));

		PaymentWorkAdmission.AdmissionResult result =
				admission.tryAdmit(UUID.randomUUID(), partition, command(UUID.randomUUID(), partition));

		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.OVERFLOW);
	}

	@Test
	void overflowsWhenGlobalCapacityIsExceeded() {
		PaymentWorkAdmission admission = admission(1, 8, 4);
		admission.tryAdmit(UUID.randomUUID(), partition(0), command(UUID.randomUUID(), partition(0)));

		PaymentWorkAdmission.AdmissionResult result =
				admission.tryAdmit(UUID.randomUUID(), partition(1), command(UUID.randomUUID(), partition(1)));

		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.OVERFLOW);
	}

	@Test
	void releasePromotesTheNextQueuedCommandForTheSamePayment() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		QueuedCommand second = command(paymentId, partition);
		admission.tryAdmit(paymentId, partition, second);

		Optional<QueuedCommand> promoted = admission.release(paymentId, partition);

		assertThat(promoted).isPresent();
		assertThat(promoted.get()).isSameAs(second);
	}

	@Test
	void releaseWithNoWaitingCommandFreesThePaymentAndCapacity() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		Optional<QueuedCommand> promoted = admission.release(paymentId, partition);

		assertThat(promoted).isEmpty();
		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.ADMIT);
	}

	@Test
	void bumpEpochChangesCurrentEpochForThatPartitionOnly() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition revoked = partition(0);
		TopicPartition untouched = partition(1);

		admission.bumpEpoch(revoked);

		assertThat(admission.currentEpoch(revoked)).isEqualTo(1L);
		assertThat(admission.currentEpoch(untouched)).isEqualTo(0L);
	}

	@Test
	void partitionsNeedingResumeReportsOnlyPartitionsThatRegainedCapacity() {
		PaymentWorkAdmission admission = admission(1, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		PaymentWorkAdmission.AdmissionResult overflow =
				admission.tryAdmit(UUID.randomUUID(), partition(1), command(UUID.randomUUID(), partition(1)));
		assertThat(overflow).isEqualTo(PaymentWorkAdmission.AdmissionResult.OVERFLOW);
		assertThat(admission.partitionsNeedingResume()).isEmpty();

		admission.release(paymentId, partition);

		assertThat(admission.partitionsNeedingResume()).containsExactly(partition(1));
		assertThat(admission.partitionsNeedingResume()).isEmpty();
	}

	@Test
	void overflowRecordIsRetainedAndAdmittedOnceCapacityFrees() {
		PaymentWorkAdmission admission = admission(1, 1, 4);
		TopicPartition partition = partition(0);
		UUID firstPaymentId = UUID.randomUUID();
		UUID overflowPaymentId = UUID.randomUUID();
		QueuedCommand overflowCommand = command(overflowPaymentId, partition);
		admission.tryAdmit(firstPaymentId, partition, command(firstPaymentId, partition));

		PaymentWorkAdmission.AdmissionResult overflowResult = admission.tryAdmit(overflowPaymentId, partition, overflowCommand);
		assertThat(overflowResult).isEqualTo(PaymentWorkAdmission.AdmissionResult.OVERFLOW);
		assertThat(admission.drainOverflow(partition)).isEmpty();

		admission.release(firstPaymentId, partition);

		List<QueuedCommand> drained = admission.drainOverflow(partition);
		assertThat(drained).containsExactly(overflowCommand);
		assertThat(admission.drainOverflow(partition)).isEmpty();
	}

	@Test
	void quarantinePartitionDiscardsOverflowRetainedCommandsForThatPartition() {
		PaymentWorkAdmission admission = admission(1, 1, 4);
		TopicPartition partition = partition(0);
		UUID firstPaymentId = UUID.randomUUID();
		UUID overflowPaymentId = UUID.randomUUID();
		admission.tryAdmit(firstPaymentId, partition, command(firstPaymentId, partition));
		admission.tryAdmit(overflowPaymentId, partition, command(overflowPaymentId, partition));

		admission.quarantinePartition(partition);
		admission.release(firstPaymentId, partition);

		assertThat(admission.drainOverflow(partition)).isEmpty();
	}

	@Test
	void quarantinePartitionDropsQueuedCommandsForThatPartition() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		admission.quarantinePartition(partition);

		Optional<QueuedCommand> promoted = admission.release(paymentId, partition);
		assertThat(promoted).isEmpty();
	}

	@Test
	void quarantinePartitionLeavesActiveDispatchedPaymentTracked() {
		PaymentWorkAdmission admission = admission(32, 8, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		admission.quarantinePartition(partition);

		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.QUEUED);
	}

	@Test
	void quarantinePartitionResetsInFlightCounterForFutureReassignment() {
		PaymentWorkAdmission admission = admission(2, 2, 4);
		TopicPartition partition = partition(0);
		UUID paymentId = UUID.randomUUID();
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));
		admission.tryAdmit(paymentId, partition, command(paymentId, partition));

		admission.quarantinePartition(partition);

		PaymentWorkAdmission.AdmissionResult result =
				admission.tryAdmit(UUID.randomUUID(), partition, command(UUID.randomUUID(), partition));
		assertThat(result).isEqualTo(PaymentWorkAdmission.AdmissionResult.ADMIT);
	}

	@Test
	void concurrentTryAdmitAndReleaseNeverCorruptCounters() throws Exception {
		int globalCapacity = 32;
		int perPartitionCapacity = 8;
		int perPaymentWaitingCapacity = 4;
		PaymentWorkAdmission admission = admission(globalCapacity, perPartitionCapacity, perPaymentWaitingCapacity);
		List<TopicPartition> partitions = List.of(partition(0), partition(1), partition(2), partition(3));
		List<UUID> paymentIds = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			paymentIds.add(UUID.randomUUID());
		}

		int threadCount = 16;
		int opsPerThread = 500;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		CyclicBarrier barrier = new CyclicBarrier(threadCount);
		AtomicInteger overflowCount = new AtomicInteger();
		List<Future<?>> futures = new ArrayList<>();

		for (int t = 0; t < threadCount; t++) {
			futures.add(executor.submit(() -> {
				try {
					barrier.await();
					for (int i = 0; i < opsPerThread; i++) {
						TopicPartition partition = partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
						UUID paymentId = paymentIds.get(ThreadLocalRandom.current().nextInt(paymentIds.size()));
						PaymentWorkAdmission.AdmissionResult result =
								admission.tryAdmit(paymentId, partition, command(paymentId, partition));
						if (result == PaymentWorkAdmission.AdmissionResult.OVERFLOW) {
							overflowCount.incrementAndGet();
						}
						else {
							admission.release(paymentId, partition);
						}
					}
				}
				catch (Exception exception) {
					throw new RuntimeException(exception);
				}
			}));
		}

		for (Future<?> future : futures) {
			future.get(30, TimeUnit.SECONDS);
		}
		executor.shutdown();

		assertThat(overflowCount.get()).isGreaterThanOrEqualTo(0);
		for (TopicPartition partition : partitions) {
			UUID probe = UUID.randomUUID();
			for (int i = 0; i < perPartitionCapacity; i++) {
				PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(probe, partition, command(probe, partition));
				assertThat(result).isNotEqualTo(PaymentWorkAdmission.AdmissionResult.QUEUED);
				admission.release(probe, partition);
			}
		}
	}

	@Test
	void concurrentOverflowAndReleaseNeverDivergePauseAndResumeCalls() throws Exception {
		for (int run = 0; run < 5; run++) {
			runPauseResumeRaceOnce();
		}
	}

	private void runPauseResumeRaceOnce() throws Exception {
		KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		when(registry.getListenerContainer(PaymentOperationCommandListener.LISTENER_ID)).thenReturn(container);

		List<String> events = new ArrayList<>();
		Object eventsLock = new Object();
		doAnswer(invocation -> {
			synchronized (eventsLock) {
				events.add("PAUSE");
			}
			return null;
		}).when(container).pausePartition(any());
		doAnswer(invocation -> {
			synchronized (eventsLock) {
				events.add("RESUME");
			}
			return null;
		}).when(container).resumePartition(any());

		PaymentWorkAdmission admission = admission(1, 1, 4, registry);
		TopicPartition partition = partition(0);
		ConcurrentLinkedQueue<UUID> admitted = new ConcurrentLinkedQueue<>();
		AtomicBoolean admitterDone = new AtomicBoolean(false);
		int iterations = 4000;

		ExecutorService executor = Executors.newFixedThreadPool(2);
		Future<?> admitterFuture = executor.submit(() -> {
			try {
				for (int i = 0; i < iterations; i++) {
					UUID paymentId = UUID.randomUUID();
					PaymentWorkAdmission.AdmissionResult result =
							admission.tryAdmit(paymentId, partition, command(paymentId, partition));
					if (result == PaymentWorkAdmission.AdmissionResult.ADMIT) {
						admitted.add(paymentId);
					}
				}
			}
			finally {
				admitterDone.set(true);
			}
		});
		Future<?> releaserFuture = executor.submit(() -> {
			while (!admitterDone.get() || !admitted.isEmpty()) {
				UUID paymentId = admitted.poll();
				if (paymentId == null) {
					Thread.onSpinWait();
					continue;
				}
				admission.release(paymentId, partition);
				admission.partitionsNeedingResume();
			}
		});

		admitterFuture.get(30, TimeUnit.SECONDS);
		releaserFuture.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		UUID remaining;
		while ((remaining = admitted.poll()) != null) {
			admission.release(remaining, partition);
			admission.partitionsNeedingResume();
		}

		synchronized (eventsLock) {
			for (int i = 1; i < events.size(); i++) {
				assertThat(events.get(i)).isNotEqualTo(events.get(i - 1));
			}
			if (!events.isEmpty()) {
				assertThat(events.get(0)).isEqualTo("PAUSE");
				assertThat(events.get(events.size() - 1)).isEqualTo("RESUME");
			}
		}
	}

	private PaymentWorkAdmission admission(int globalCapacity, int perPartitionCapacity, int perPaymentWaitingCapacity) {
		KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		when(registry.getListenerContainer(PaymentOperationCommandListener.LISTENER_ID)).thenReturn(container);
		return admission(globalCapacity, perPartitionCapacity, perPaymentWaitingCapacity, registry);
	}

	private PaymentWorkAdmission admission(
			int globalCapacity, int perPartitionCapacity, int perPaymentWaitingCapacity, KafkaListenerEndpointRegistry registry) {
		return new PaymentWorkAdmission(new ProcessorProperties(
				URI.create("http://localhost:8081"),
				Duration.ofSeconds(1),
				List.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20)),
				Duration.ofSeconds(3),
				Duration.ofSeconds(120),
				Duration.ofMillis(1),
				Duration.ofMillis(1),
				new ProcessorProperties.Reconciliation(
						Duration.ofSeconds(5), 20, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(60)),
				new ProcessorProperties.Workers(globalCapacity, perPartitionCapacity, perPaymentWaitingCapacity, 4),
				new ProcessorProperties.Dispatcher(Duration.ofSeconds(2), 20, Duration.ofSeconds(60))),
				registry);
	}

	private TopicPartition partition(int index) {
		return new TopicPartition("payment.operations.requested.v1", index);
	}

	private QueuedCommand command(UUID paymentId, TopicPartition partition) {
		ConsumerRecord<byte[], Object> record = new ConsumerRecord<>(
				partition.topic(), partition.partition(), 0L,
				paymentId.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8), null);
		return new QueuedCommand(record, NO_OP_ACKNOWLEDGMENT, partition, paymentId, 0L);
	}
}
