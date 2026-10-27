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
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PaymentOperationVoidAttemptExecutorTests {

	private static final UUID OPERATION_ID = UUID.randomUUID();
	private static final UUID AUTHORIZATION_PSP_OPERATION_ID = UUID.randomUUID();
	private static final UUID CORRELATION_ID = UUID.randomUUID();
	private static final UUID CAUSATION_ID = UUID.randomUUID();
	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	private final PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
	private final PspVoidClient attemptClient1 = mock(PspVoidClient.class);
	private final PspVoidClient attemptClient2 = mock(PspVoidClient.class);
	private final PspVoidClient attemptClient3 = mock(PspVoidClient.class);
	private final PspLookupClient lookupClient = mock(PspLookupClient.class);

	@Test
	void confirmedVoidCommitsSucceededOnFirstAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(OPERATION_ID.toString());
		when(attemptClient1.voidOperation(any(), eq(AUTHORIZATION_PSP_OPERATION_ID))).thenReturn(outcome);
		when(repository.commitOutcome(eq(OPERATION_ID), any(), eq(outcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1).voidOperation(any(), eq(AUTHORIZATION_PSP_OPERATION_ID));
		verify(attemptClient2, never()).voidOperation(any(), any());
		verify(attemptClient3, never()).voidOperation(any(), any());
	}

	@Test
	void explicitRejectionCommitsRejectedWithoutMarkingPaymentCanceled() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 0, null));
		when(repository.reserveAttempt(eq(OPERATION_ID), any(), any(), any())).thenReturn(Optional.of(1));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.rejected(OPERATION_ID.toString(), "NON_EXECUTION_CONFIRMED");
		when(attemptClient1.voidOperation(any(), eq(AUTHORIZATION_PSP_OPERATION_ID))).thenReturn(outcome);
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
		when(attemptClient1.voidOperation(any(), any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(attemptClient2.voidOperation(any(), any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(attemptClient3.voidOperation(any(), any())).thenReturn(PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"));
		when(repository.commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), any(), eq("PSP_UNREACHABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1).voidOperation(any(), any());
		verify(attemptClient2).voidOperation(any(), any());
		verify(attemptClient3).voidOperation(any(), any());
		verify(repository).commitUnknownWithReconciliationJob(
				eq(OPERATION_ID), any(), eq("PSP_UNREACHABLE"), eq(CORRELATION_ID), eq(CAUSATION_ID), any());
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
		verify(attemptClient1, never()).voidOperation(any(), any());
		verify(repository, never()).reserveAttempt(any(), any(), any(), any());
	}

	@Test
	void takeoverWithDefinitiveLookupShortCircuitsWithoutAnyFurtherAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.ACQUIRED, 1, UUID.randomUUID()));
		PspAuthorizationOutcome lookupOutcome = PspAuthorizationOutcome.succeeded(OPERATION_ID.toString());
		when(lookupClient.lookup(OPERATION_ID)).thenReturn(lookupOutcome);
		when(repository.commitOutcome(eq(OPERATION_ID), any(), eq(lookupOutcome), eq(CORRELATION_ID), eq(CAUSATION_ID), any()))
				.thenReturn(true);

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isTrue();
		verify(attemptClient1, never()).voidOperation(any(), any());
	}

	@Test
	void deniedLeaseWithholdsAcknowledgmentWithoutAnyAttempt() {
		when(repository.claimOrRenewLease(eq(OPERATION_ID), any(), eq(NOW), any())).thenReturn(
				new PaymentOperationProcessingRepository.LeaseClaim(
						PaymentOperationProcessingRepository.LeaseClaim.Status.DENIED, 1, UUID.randomUUID()));

		boolean ackEligible = executor().execute(context(), CORRELATION_ID, CAUSATION_ID);

		assertThat(ackEligible).isFalse();
		verify(attemptClient1, never()).voidOperation(any(), any());
		verify(lookupClient, never()).lookup(any());
	}

	private PaymentOperationVoidAttemptExecutor executor() {
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
		return new PaymentOperationVoidAttemptExecutor(
				repository,
				List.of(attemptClient1, attemptClient2, attemptClient3),
				lookupClient,
				new ProcessorIdentity(),
				properties,
				Clock.fixed(NOW, ZoneOffset.UTC));
	}

	private PaymentOperationContext context() {
		return new PaymentOperationContext(
				OPERATION_ID, "VOID", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "AUTHORIZED",
				0, null, false, AUTHORIZATION_PSP_OPERATION_ID.toString());
	}

	private PaymentOperationContext contextWithoutAuthorizationReference() {
		return new PaymentOperationContext(
				OPERATION_ID, "VOID", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "AUTHORIZED",
				0, null, false, null);
	}
}
