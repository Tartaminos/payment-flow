package jhsoftware.paymentsflow.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class UnknownOperationReconciliationScannerTests {

	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	private final ReconciliationJobRepository repository = mock(ReconciliationJobRepository.class);
	private final PspLookupClient lookupClient = mock(PspLookupClient.class);
	private final ReconciliationScannerHealth health = mock(ReconciliationScannerHealth.class);

	@Test
	void authorizeUnknownJobIsClaimedAndResolvedUsingTheOriginalPspIdentity() {
		UUID jobId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, operationId, 1)));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(operationId.toString());
		when(lookupClient.lookup(operationId)).thenReturn(outcome);
		when(repository.resolveJob(eq(jobId), any(), eq(operationId), eq(outcome), any(), any(), any())).thenReturn(true);

		scanner().scan();

		verify(lookupClient).lookup(operationId);
		verify(repository).resolveJob(eq(jobId), any(), eq(operationId), eq(outcome), any(), any(), any());
	}

	@Test
	void captureUnknownJobIsClaimedAndResolvedJustLikeAuthorizeUnknown() {
		UUID jobId = UUID.randomUUID();
		UUID captureOperationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, captureOperationId, 2)));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(captureOperationId.toString());
		when(lookupClient.lookup(captureOperationId)).thenReturn(outcome);
		when(repository.resolveJob(eq(jobId), any(), eq(captureOperationId), eq(outcome), any(), any(), any())).thenReturn(true);

		scanner().scan();

		verify(lookupClient).lookup(captureOperationId);
		verify(repository).resolveJob(eq(jobId), any(), eq(captureOperationId), eq(outcome), any(), any(), any());
	}

	@Test
	void ambiguousLookupBacksOffTheJobRatherThanResolvingIt() {
		UUID jobId = UUID.randomUUID();
		UUID captureOperationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, captureOperationId, 0)));
		when(lookupClient.lookup(captureOperationId)).thenReturn(PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND"));
		when(repository.bumpBackoff(eq(jobId), any(), eq("PSP_LOOKUP_NOT_FOUND"), any(), any())).thenReturn(true);

		scanner().scan();

		verify(repository).bumpBackoff(eq(jobId), any(), eq("PSP_LOOKUP_NOT_FOUND"), any(), any());
		verify(repository, never()).resolveJob(any(), any(), any(), any(), any(), any(), any());
	}

	private UnknownOperationReconciliationScanner scanner() {
		ProcessorProperties properties = new ProcessorProperties(
				URI.create("http://localhost:8081"),
				Duration.ofSeconds(1),
				List.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20)),
				Duration.ofSeconds(3),
				Duration.ofSeconds(120),
				Duration.ofMillis(1),
				Duration.ofMillis(1),
				new ProcessorProperties.Reconciliation(
						Duration.ofSeconds(5), 20, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(60)),
				new ProcessorProperties.Workers(32, 8, 4, 4));
		return new UnknownOperationReconciliationScanner(
				repository, lookupClient, new ProcessorIdentity(), properties, Clock.fixed(NOW, ZoneOffset.UTC), health);
	}
}
