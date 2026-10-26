package jhsoftware.paymentsflow.processor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentWorkAdmission {

	enum AdmissionResult {
		ADMIT, QUEUED, OVERFLOW
	}

	private final Object lock = new Object();
	private final int globalCapacity;
	private final int perPartitionCapacity;
	private final int perPaymentWaitingCapacity;
	private final KafkaListenerEndpointRegistry registry;

	private int globalInFlight;
	private final Map<TopicPartition, Integer> partitionInFlight = new HashMap<>();
	private final Map<UUID, Deque<QueuedCommand>> waitingByPayment = new HashMap<>();
	private final Set<UUID> activePayments = new HashSet<>();
	private final Map<TopicPartition, Long> epochByPartition = new HashMap<>();
	private final Set<TopicPartition> capacityPaused = new HashSet<>();
	private final Map<TopicPartition, Deque<QueuedCommand>> overflowByPartition = new HashMap<>();

	PaymentWorkAdmission(ProcessorProperties properties, KafkaListenerEndpointRegistry registry) {
		this.globalCapacity = properties.workers().globalCapacity();
		this.perPartitionCapacity = properties.workers().perPartitionCapacity();
		this.perPaymentWaitingCapacity = properties.workers().perPaymentWaitingCapacity();
		this.registry = registry;
	}

	AdmissionResult tryAdmit(UUID paymentId, TopicPartition partition, QueuedCommand command) {
		synchronized (lock) {
			AdmissionResult result = admitInternal(paymentId, partition, command);
			if (result == AdmissionResult.OVERFLOW) {
				pausePartition(partition);
				overflowByPartition.computeIfAbsent(partition, key -> new ArrayDeque<>()).addLast(command);
			}
			return result;
		}
	}

	List<QueuedCommand> drainOverflow(TopicPartition partition) {
		synchronized (lock) {
			List<QueuedCommand> toDispatch = new ArrayList<>();
			Deque<QueuedCommand> overflow = overflowByPartition.get(partition);
			if (overflow == null) {
				return toDispatch;
			}

			while (!overflow.isEmpty()) {
				QueuedCommand candidate = overflow.peekFirst();
				AdmissionResult result = admitInternal(candidate.paymentId(), partition, candidate);
				if (result == AdmissionResult.OVERFLOW) {
					break;
				}
				overflow.pollFirst();
				if (result == AdmissionResult.ADMIT) {
					toDispatch.add(candidate);
				}
			}

			if (overflow.isEmpty()) {
				overflowByPartition.remove(partition);
			}
			return toDispatch;
		}
	}

	private AdmissionResult admitInternal(UUID paymentId, TopicPartition partition, QueuedCommand command) {
		int partitionCount = partitionInFlight.getOrDefault(partition, 0);
		if (globalInFlight >= globalCapacity || partitionCount >= perPartitionCapacity) {
			return AdmissionResult.OVERFLOW;
		}

		if (activePayments.contains(paymentId)) {
			Deque<QueuedCommand> waiting = waitingByPayment.computeIfAbsent(paymentId, key -> new ArrayDeque<>());
			if (waiting.size() >= perPaymentWaitingCapacity) {
				return AdmissionResult.OVERFLOW;
			}
			waiting.addLast(command);
			admitCounters(partition);
			return AdmissionResult.QUEUED;
		}

		activePayments.add(paymentId);
		admitCounters(partition);
		return AdmissionResult.ADMIT;
	}

	Optional<QueuedCommand> release(UUID paymentId, TopicPartition partition) {
		synchronized (lock) {
			globalInFlight--;
			partitionInFlight.computeIfPresent(partition, (key, count) -> count <= 1 ? null : count - 1);

			Deque<QueuedCommand> waiting = waitingByPayment.get(paymentId);
			if (waiting != null && !waiting.isEmpty()) {
				QueuedCommand next = waiting.pollFirst();
				if (waiting.isEmpty()) {
					waitingByPayment.remove(paymentId);
				}
				return Optional.of(next);
			}

			activePayments.remove(paymentId);
			waitingByPayment.remove(paymentId);
			return Optional.empty();
		}
	}

	void quarantinePartition(TopicPartition partition) {
		synchronized (lock) {
			int removed = 0;
			Iterator<Map.Entry<UUID, Deque<QueuedCommand>>> entries = waitingByPayment.entrySet().iterator();
			while (entries.hasNext()) {
				Map.Entry<UUID, Deque<QueuedCommand>> entry = entries.next();
				Deque<QueuedCommand> waiting = entry.getValue();
				Iterator<QueuedCommand> commands = waiting.iterator();
				while (commands.hasNext()) {
					if (commands.next().partition().equals(partition)) {
						commands.remove();
						removed++;
					}
				}
				if (waiting.isEmpty()) {
					entries.remove();
				}
			}
			globalInFlight -= removed;
			partitionInFlight.remove(partition);
			overflowByPartition.remove(partition);
			if (capacityPaused.remove(partition)) {
				resumePartition(partition);
			}
		}
	}

	void bumpEpoch(TopicPartition partition) {
		synchronized (lock) {
			epochByPartition.merge(partition, 1L, Long::sum);
		}
	}

	long currentEpoch(TopicPartition partition) {
		synchronized (lock) {
			return epochByPartition.getOrDefault(partition, 0L);
		}
	}

	Set<TopicPartition> partitionsNeedingResume() {
		synchronized (lock) {
			Set<TopicPartition> ready = new HashSet<>();
			Iterator<TopicPartition> iterator = capacityPaused.iterator();
			while (iterator.hasNext()) {
				TopicPartition partition = iterator.next();
				int partitionCount = partitionInFlight.getOrDefault(partition, 0);
				if (globalInFlight < globalCapacity && partitionCount < perPartitionCapacity) {
					ready.add(partition);
					iterator.remove();
					resumePartition(partition);
				}
			}
			return ready;
		}
	}

	private void admitCounters(TopicPartition partition) {
		globalInFlight++;
		partitionInFlight.merge(partition, 1, Integer::sum);
	}

	private void pausePartition(TopicPartition partition) {
		if (capacityPaused.add(partition)) {
			registry.getListenerContainer(PaymentOperationCommandListener.LISTENER_ID).pausePartition(partition);
		}
	}

	private void resumePartition(TopicPartition partition) {
		registry.getListenerContainer(PaymentOperationCommandListener.LISTENER_ID).resumePartition(partition);
	}
}
