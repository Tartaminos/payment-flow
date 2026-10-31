package jhsoftware.paymentsflow.processor.observability;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class BlockedPartitionDiagnostics {

	private static final Logger logger = LoggerFactory.getLogger(BlockedPartitionDiagnostics.class);
	private static final Duration BLOCKED_THRESHOLD = Duration.ofSeconds(30);

	private final Clock clock;
	private final Map<TopicPartition, Instant> ineligibleSince = new ConcurrentHashMap<>();

	BlockedPartitionDiagnostics(Clock clock) {
		this.clock = clock;
	}

	public void recordIneligible(TopicPartition partition, Instant now) {
		ineligibleSince.putIfAbsent(partition, now);
	}

	public void recordEligible(TopicPartition partition) {
		ineligibleSince.remove(partition);
	}

	boolean isPotentiallyBlocked(TopicPartition partition, Instant now, Duration threshold) {
		Instant since = ineligibleSince.get(partition);
		return since != null && Duration.between(since, now).compareTo(threshold) >= 0;
	}

	@Scheduled(fixedDelay = 10000, initialDelay = 10000)
	void logPotentiallyBlockedPartitions() {
		Instant now = clock.instant();
		for (Map.Entry<TopicPartition, Instant> entry : ineligibleSince.entrySet()) {
			if (isPotentiallyBlocked(entry.getKey(), now, BLOCKED_THRESHOLD)) {
				logger.warn(
						"Partition {} has withheld acknowledgment since {}; it may be blocked on a record that cannot become eligible before M5",
						entry.getKey(),
						entry.getValue());
			}
		}
	}
}
