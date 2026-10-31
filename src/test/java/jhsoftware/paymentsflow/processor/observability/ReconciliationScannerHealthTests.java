package jhsoftware.paymentsflow.processor.observability;

import jhsoftware.paymentsflow.processor.config.ProcessorProperties;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

class ReconciliationScannerHealthTests {

	private static final Instant START = Instant.parse("2026-10-25T10:00:00Z");

	@Test
	void isNotOperationalBeforeAnyHeartbeatIsRecorded() {
		ReconciliationScannerHealth health = health(Duration.ofSeconds(5));

		assertThat(health.isOperational(START)).isFalse();
	}

	@Test
	void isOperationalWithinTheStalenessThresholdAfterAHeartbeat() {
		ReconciliationScannerHealth health = health(Duration.ofSeconds(5));
		health.recordHeartbeat(START);

		assertThat(health.isOperational(START.plusSeconds(15))).isTrue();
	}

	@Test
	void isNotOperationalOnceTheHeartbeatIsStale() {
		ReconciliationScannerHealth health = health(Duration.ofSeconds(5));
		health.recordHeartbeat(START);

		assertThat(health.isOperational(START.plusSeconds(15).plusMillis(1))).isFalse();
	}

	private ReconciliationScannerHealth health(Duration pollInterval) {
		ProcessorProperties properties = new ProcessorProperties(
				URI.create("http://localhost:8081"),
				Duration.ofSeconds(1),
				List.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20)),
				Duration.ofSeconds(3),
				Duration.ofSeconds(120),
				Duration.ofMillis(1),
				Duration.ofMillis(1),
				new ProcessorProperties.Reconciliation(
						pollInterval, 20, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(60)),
				new ProcessorProperties.Workers(32, 8, 4, 4),
				new ProcessorProperties.Dispatcher(Duration.ofSeconds(2), 20, Duration.ofSeconds(60)));
		return new ReconciliationScannerHealth(properties);
	}
}
