package jhsoftware.paymentsflow.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class RetentionCleanupJobTests {

	private static final Instant NOW = Instant.parse("2026-10-27T03:00:00Z");

	private final RetentionCleanupRepository repository = mock(RetentionCleanupRepository.class);

	@Test
	void dryRunComputesCandidatesButNeverDeletes() {
		List<UUID> candidates = List.of(UUID.randomUUID());
		when(repository.findCleanupCandidatePaymentIds(eq(NOW), any())).thenReturn(candidates);

		job(true).cleanup();

		verify(repository).findCleanupCandidatePaymentIds(NOW, Period.ofYears(5));
		verify(repository, never()).deleteCleanupCandidates(any());
	}

	@Test
	void disabledDryRunActuallyDeletesTheComputedCandidates() {
		List<UUID> candidates = List.of(UUID.randomUUID(), UUID.randomUUID());
		when(repository.findCleanupCandidatePaymentIds(eq(NOW), any())).thenReturn(candidates);
		when(repository.deleteCleanupCandidates(candidates)).thenReturn(3);

		job(false).cleanup();

		verify(repository).deleteCleanupCandidates(candidates);
	}

	private RetentionCleanupJob job(boolean dryRun) {
		RetentionCleanupProperties properties = new RetentionCleanupProperties(
				new RetentionCleanupProperties.Cleanup(true, dryRun, Period.ofYears(5), "0 0 3 1 * *"));
		return new RetentionCleanupJob(repository, properties, Clock.fixed(NOW, ZoneOffset.UTC));
	}
}
