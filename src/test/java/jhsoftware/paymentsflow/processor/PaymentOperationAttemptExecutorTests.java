package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PaymentOperationAttemptExecutorTests {

	private static final UUID OPERATION_ID = UUID.randomUUID();
	private static final UUID CORRELATION_ID = UUID.randomUUID();
	private static final UUID CAUSATION_ID = UUID.randomUUID();
	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	private final PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
	private final PspAuthorizationClient attemptClient1 = mock(PspAuthorizationClient.class);
	private final PspAuthorizationClient attemptClient2 = mock(PspAuthorizationClient.class);
	private final PspAuthorizationClient attemptClient3 = mock(PspAuthorizationClient.class);
	private final PspLookupClient lookupClient = mock(PspLookupClient.class);

	@Test
	void takeoverWithDefinitiveLookupCommitsWithoutAnyAttemptPost() {
		UUID previousOwner = UUID.randomUUID();
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 2, previousOwner));
		PspAuthorizationOutcome lookupOutcome = PspAuthorizationOutcome.succeeded(OPERATION_ID.toString());
		when(lookupClient.lookup(OPERATION_ID)).thenReturn(lookupOutcome);
		when(repository.commitOutcome(OPERATION_ID, lookupOutcome, CORRELATION_ID, CAUSATION_ID, NOW)).thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(lookupClient).lookup(OPERATION_ID);
		verify(attemptClient1, never()).authorize(any());
		verify(attemptClient2, never()).authorize(any());
		verify(attemptClient3, never()).authorize(any());
		verify(repository, never()).reserveAttempt(any(), any(), any(), any());
	}

	@Test
	void takeoverWithAmbiguousLookupContinuesIntoRemainingAttemptBudget() {
		UUID previousOwner = UUID.randomUUID();
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 2, previousOwner));
		when(lookupClient.lookup(OPERATION_ID)).thenReturn(PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND"));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(Optional.of(3));
		PspAuthorizationOutcome attemptOutcome = PspAuthorizationOutcome.succeeded(OPERATION_ID.toString());
		when(attemptClient3.authorize(any())).thenReturn(attemptOutcome);
		when(repository.commitOutcome(OPERATION_ID, attemptOutcome, CORRELATION_ID, CAUSATION_ID, NOW)).thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient3).authorize(any());
		verify(attemptClient1, never()).authorize(any());
		verify(attemptClient2, never()).authorize(any());
	}

	@Test
	void budgetExhaustionWithAllAmbiguousAttemptsCommitsUnknownWithReconciliationJob() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any()))
				.thenReturn(Optional.of(1))
				.thenReturn(Optional.of(2))
				.thenReturn(Optional.of(3));
		when(attemptClient1.authorize(any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(attemptClient2.authorize(any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(attemptClient3.authorize(any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(repository.commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), eq("PSP_UNREACHABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1).authorize(any());
		verify(attemptClient2).authorize(any());
		verify(attemptClient3).authorize(any());
		verify(repository).commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), eq("PSP_UNREACHABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any());
	}

	@Test
	void definitiveOutcomeOnFirstAttemptStopsTheLoop() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.rejected(OPERATION_ID.toString(), "CARD_DECLINED");
		when(attemptClient1.authorize(any())).thenReturn(outcome);
		when(repository.commitOutcome(eq(OPERATION_ID), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1).authorize(any());
		verify(attemptClient2, never()).authorize(any());
		verify(attemptClient3, never()).authorize(any());
		verify(repository, times(1)).reserveAttempt(any(), any(), any(), any());
	}

	@Test
	void deniedLeaseWithholdsAcknowledgmentWithoutAnyAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.DENIED, 1, UUID.randomUUID()));

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isFalse();
		verify(attemptClient1, never()).authorize(any());
		verify(lookupClient, never()).lookup(any());
		verify(repository, never()).reserveAttempt(any(), any(), any(), any());
	}

	private PaymentOperationAttemptExecutor executor() {
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
		return new PaymentOperationAttemptExecutor(
				repository,
				List.of(attemptClient1, attemptClient2, attemptClient3),
				lookupClient,
				new ProcessorIdentity(),
				properties,
				Clock.fixed(NOW, ZoneOffset.UTC));
	}

	private PaymentOperationContext context() {
		return new PaymentOperationContext(
				OPERATION_ID, "AUTHORIZE", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "CREATED",
				0, null, false);
	}
}
