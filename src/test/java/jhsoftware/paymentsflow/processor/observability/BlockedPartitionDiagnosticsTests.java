package jhsoftware.paymentsflow.processor.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BlockedPartitionDiagnosticsTests {

	private static final TopicPartition PARTITION = new TopicPartition("payment.operations.requested.v1", 0);
	private static final Instant START = Instant.parse("2026-10-25T10:00:00Z");

	@Test
	void isNotPotentiallyBlockedWhenNoIneligibleAcknowledgmentWasRecorded() {
		BlockedPartitionDiagnostics diagnostics = diagnostics();

		assertThat(diagnostics.isPotentiallyBlocked(PARTITION, START, Duration.ofSeconds(30))).isFalse();
	}

	@Test
	void isNotPotentiallyBlockedBeforeTheThresholdElapses() {
		BlockedPartitionDiagnostics diagnostics = diagnostics();
		diagnostics.recordIneligible(PARTITION, START);

		assertThat(diagnostics.isPotentiallyBlocked(PARTITION, START.plusSeconds(10), Duration.ofSeconds(30))).isFalse();
	}

	@Test
	void isPotentiallyBlockedOnceTheThresholdElapses() {
		BlockedPartitionDiagnostics diagnostics = diagnostics();
		diagnostics.recordIneligible(PARTITION, START);

		assertThat(diagnostics.isPotentiallyBlocked(PARTITION, START.plusSeconds(30), Duration.ofSeconds(30))).isTrue();
	}

	@Test
	void recordEligibleClearsThePreviouslyTrackedIneligibility() {
		BlockedPartitionDiagnostics diagnostics = diagnostics();
		diagnostics.recordIneligible(PARTITION, START);
		diagnostics.recordEligible(PARTITION);

		assertThat(diagnostics.isPotentiallyBlocked(PARTITION, START.plusSeconds(60), Duration.ofSeconds(30))).isFalse();
	}

	@Test
	void repeatedIneligibleRecordingsDoNotResetTheOriginalStartTime() {
		BlockedPartitionDiagnostics diagnostics = diagnostics();
		diagnostics.recordIneligible(PARTITION, START);
		diagnostics.recordIneligible(PARTITION, START.plusSeconds(20));

		assertThat(diagnostics.isPotentiallyBlocked(PARTITION, START.plusSeconds(30), Duration.ofSeconds(30))).isTrue();
	}

	private BlockedPartitionDiagnostics diagnostics() {
		return new BlockedPartitionDiagnostics(Clock.fixed(START, ZoneOffset.UTC));
	}
}
