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

class PaymentOperationCaptureAttemptExecutorTests {

	private static final UUID OPERATION_ID = UUID.randomUUID();
	private static final UUID AUTHORIZATION_PSP_OPERATION_ID = UUID.randomUUID();
	private static final UUID CORRELATION_ID = UUID.randomUUID();
	private static final UUID CAUSATION_ID = UUID.randomUUID();
	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	private final PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
	private final PspCaptureClient attemptClient1 = mock(PspCaptureClient.class);
	private final PspCaptureClient attemptClient2 = mock(PspCaptureClient.class);
	private final PspCaptureClient attemptClient3 = mock(PspCaptureClient.class);
	private final PspLookupClient lookupClient = mock(PspLookupClient.class);

	@Test
	void confirmedCaptureCommitsSucceededOnFirstAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(OPERATION_ID.toString());
		when(attemptClient1.capture(any(), eq(AUTHORIZATION_PSP_OPERATION_ID))).thenReturn(outcome);
		when(repository.commitOutcome(eq(OPERATION_ID), any(), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1).capture(any(), eq(AUTHORIZATION_PSP_OPERATION_ID));
		verify(attemptClient2, never()).capture(any(), any());
		verify(attemptClient3, never()).capture(any(), any());
	}

	@Test
	void pspRejectionCommitsRejectedWithoutMarkingPaymentCaptured() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.rejected(OPERATION_ID.toString(), "CARD_DECLINED");
		when(attemptClient1.capture(any(), eq(AUTHORIZATION_PSP_OPERATION_ID))).thenReturn(outcome);
		when(repository.commitOutcome(eq(OPERATION_ID), any(), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(repository).commitOutcome(eq(OPERATION_ID), any(), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any());
	}

	@Test
	void pspExpiredAuthorizationRejectionCommitsRejectedWithExpiryReason() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.rejected(OPERATION_ID.toString(), "AUTHORIZATION_EXPIRED");
		when(attemptClient1.capture(any(), eq(AUTHORIZATION_PSP_OPERATION_ID))).thenReturn(outcome);
		when(repository.commitOutcome(eq(OPERATION_ID), any(), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(repository).commitOutcome(eq(OPERATION_ID), any(), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any());
	}

	@Test
	void boundedAttemptExhaustionCommitsUnknownWithReconciliationJob() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any()))
				.thenReturn(Optional.of(1))
				.thenReturn(Optional.of(2))
				.thenReturn(Optional.of(3));
		when(attemptClient1.capture(any(), any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(attemptClient2.capture(any(), any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(attemptClient3.capture(any(), any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(repository.commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), any(), eq("PSP_UNREACHABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1).capture(any(), any());
		verify(attemptClient2).capture(any(), any());
		verify(attemptClient3).capture(any(), any());
		verify(repository).commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), any(), eq("PSP_UNREACHABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any());
	}

	@Test
	void validityRecheckDoneInsidePspClientCanShortCircuitWithoutAnyFurtherAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome expiredOutcome =
				PspAuthorizationOutcome.rejected(OPERATION_ID.toString(), "AUTHORIZATION_EXPIRED");
		when(attemptClient1.capture(any(), eq(AUTHORIZATION_PSP_OPERATION_ID))).thenReturn(expiredOutcome);
		when(repository.commitOutcome(eq(OPERATION_ID), any(), eq(expiredOutcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1, times(1)).capture(any(), eq(AUTHORIZATION_PSP_OPERATION_ID));
		verify(attemptClient2, never()).capture(any(), any());
	}

	@Test
	void missingAuthorizationReferenceCommitsUnknownWithoutCallingAnyPspClient() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), any(), eq("AUTHORIZATION_REFERENCE_UNAVAILABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(contextWithoutAuthorizationReference(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1, never()).capture(any(), any());
		verify(repository, never()).reserveAttempt(any(), any(), any(), any());
	}

	@Test
	void deniedLeaseWithholdsAcknowledgmentWithoutAnyAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.DENIED, 1, UUID.randomUUID()));

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isFalse();
		verify(attemptClient1, never()).capture(any(), any());
		verify(lookupClient, never()).lookup(any());
	}

	private PaymentOperationCaptureAttemptExecutor executor() {
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
		return new PaymentOperationCaptureAttemptExecutor(
				repository,
				List.of(attemptClient1, attemptClient2, attemptClient3),
				lookupClient,
				new ProcessorIdentity(),
				properties,
				Clock.fixed(NOW, ZoneOffset.UTC));
	}

	private PaymentOperationContext context() {
		return new PaymentOperationContext(
				OPERATION_ID, "CAPTURE", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "AUTHORIZED",
				0, null, false, AUTHORIZATION_PSP_OPERATION_ID.toString());
	}

	private PaymentOperationContext contextWithoutAuthorizationReference() {
		return new PaymentOperationContext(
				OPERATION_ID, "CAPTURE", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "AUTHORIZED",
				0, null, false, null);
	}
}
