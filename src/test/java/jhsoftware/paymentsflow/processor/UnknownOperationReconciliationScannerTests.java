package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.mockito.ArgumentCaptor;

class UnknownOperationReconciliationScannerTests {

	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	private final ReconciliationJobRepository repository = mock(ReconciliationJobRepository.class);
	private final ReconciliationEscalationRepository escalationRepository = mock(ReconciliationEscalationRepository.class);
	private final PspLookupClient lookupClient = mock(PspLookupClient.class);
	private final ReconciliationScannerHealth health = mock(ReconciliationScannerHealth.class);

	@Test
	void authorizeUnknownJobIsClaimedAndResolvedUsingTheOriginalPspIdentity() {
		UUID jobId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, operationId, 1, NOW.minusSeconds(60))));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(operationId.toString());
		when(lookupClient.lookup(operationId)).thenReturn(outcome);
		when(repository.resolveJob(eq(jobId), any(), eq(operationId), eq(outcome), any(), any(), any()))
				.thenReturn(ReconciliationJobRepository.ResolutionOutcome.RESOLVED);

		scanner().scan();

		verify(lookupClient).lookup(operationId);
		verify(repository).resolveJob(eq(jobId), any(), eq(operationId), eq(outcome), any(), any(), any());
	}

	@Test
	void captureUnknownJobIsClaimedAndResolvedJustLikeAuthorizeUnknown() {
		UUID jobId = UUID.randomUUID();
		UUID captureOperationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, captureOperationId, 2, NOW.minusSeconds(60))));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(captureOperationId.toString());
		when(lookupClient.lookup(captureOperationId)).thenReturn(outcome);
		when(repository.resolveJob(eq(jobId), any(), eq(captureOperationId), eq(outcome), any(), any(), any()))
				.thenReturn(ReconciliationJobRepository.ResolutionOutcome.RESOLVED);

		scanner().scan();

		verify(lookupClient).lookup(captureOperationId);
		verify(repository).resolveJob(eq(jobId), any(), eq(captureOperationId), eq(outcome), any(), any(), any());
	}

	@Test
	void contradictionResolutionFlagsTheJobInsteadOfLeavingItSilentlyResolved() {
		UUID jobId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, operationId, 1, NOW.minusSeconds(60))));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.rejected(operationId.toString(), "CARD_DECLINED");
		when(lookupClient.lookup(operationId)).thenReturn(outcome);
		when(repository.resolveJob(eq(jobId), any(), eq(operationId), eq(outcome), any(), any(), any()))
				.thenReturn(ReconciliationJobRepository.ResolutionOutcome.CONTRADICTION);
		when(repository.flagContradiction(eq(jobId), any(), eq(operationId), any(), any(), eq("PSP_EVIDENCE_CONTRADICTION")))
				.thenReturn(true);

		scanner().scan();

		ArgumentCaptor<Instant> nextCheckAtCaptor = ArgumentCaptor.forClass(Instant.class);
		verify(repository).flagContradiction(
				eq(jobId), any(), eq(operationId), any(), nextCheckAtCaptor.capture(), eq("PSP_EVIDENCE_CONTRADICTION"));
		assertThat(nextCheckAtCaptor.getValue()).isEqualTo(NOW.plusSeconds(10));
	}

	@Test
	void ambiguousLookupBacksOffTheJobRatherThanResolvingIt() {
		UUID jobId = UUID.randomUUID();
		UUID captureOperationId = UUID.randomUUID();
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, captureOperationId, 0, NOW.minusSeconds(60))));
		when(lookupClient.lookup(captureOperationId)).thenReturn(PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND"));
		when(repository.bumpBackoff(eq(jobId), any(), eq("PSP_LOOKUP_NOT_FOUND"), any(), any())).thenReturn(true);

		scanner().scan();

		verify(repository).bumpBackoff(eq(jobId), any(), eq("PSP_LOOKUP_NOT_FOUND"), any(), any());
		verify(repository, never()).resolveJob(any(), any(), any(), any(), any(), any(), any());
	}

	@Test
	void scanNeverRunsMoreThanFourConcurrentLookups() throws Exception {
		int jobCount = 20;
		java.util.List<ReconciliationJobRepository.ReconciliationJobClaim> claims = new java.util.ArrayList<>();
		for (int i = 0; i < jobCount; i++) {
			claims.add(new ReconciliationJobRepository.ReconciliationJobClaim(
					UUID.randomUUID(), UUID.randomUUID(), 0, NOW.minusSeconds(60)));
		}
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(claims);

		java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();
		java.util.concurrent.atomic.AtomicInteger highWaterMark = new java.util.concurrent.atomic.AtomicInteger();
		when(lookupClient.lookup(any())).thenAnswer(invocation -> {
			int current = inFlight.incrementAndGet();
			highWaterMark.getAndUpdate(previous -> Math.max(previous, current));
			try {
				Thread.sleep(20);
			}
			finally {
				inFlight.decrementAndGet();
			}
			return PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND");
		});
		when(repository.bumpBackoff(any(), any(), any(), any(), any())).thenReturn(true);

		scanner().scan();

		assertThat(highWaterMark.get()).isLessThanOrEqualTo(4);
	}

	@Test
	void jobJustUnderTwentyFourHoursUnknownIsNotEscalated() {
		UUID jobId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		Instant unknownSince = NOW.minus(Duration.ofHours(24)).plusSeconds(1);
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, operationId, 0, unknownSince)));
		when(lookupClient.lookup(operationId)).thenReturn(PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND"));
		when(repository.bumpBackoff(eq(jobId), any(), any(), any(), any())).thenReturn(true);

		scanner().scan();

		verify(escalationRepository, never()).ensureEscalation(any(), any(), any(), any());
	}

	@Test
	void jobExactlyTwentyFourHoursUnknownIsEscalated() {
		UUID jobId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		Instant unknownSince = NOW.minus(Duration.ofHours(24));
		when(repository.claimDueJobs(any(), eq(NOW), eq(20), any())).thenReturn(
				List.of(new ReconciliationJobRepository.ReconciliationJobClaim(jobId, operationId, 0, unknownSince)));
		when(lookupClient.lookup(operationId)).thenReturn(PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND"));
		when(repository.bumpBackoff(eq(jobId), any(), any(), any(), any())).thenReturn(true);
		when(escalationRepository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", NOW)).thenReturn(true);

		scanner().scan();

		verify(escalationRepository).ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", NOW);
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
				new ProcessorProperties.Workers(32, 8, 4, 4),
				new ProcessorProperties.Dispatcher(Duration.ofSeconds(2), 20, Duration.ofSeconds(60)));
		return new UnknownOperationReconciliationScanner(
				repository, escalationRepository, lookupClient, new ProcessorIdentity(), properties,
				Clock.fixed(NOW, ZoneOffset.UTC), health);
	}
}
