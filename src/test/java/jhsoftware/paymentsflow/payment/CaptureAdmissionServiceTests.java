package jhsoftware.paymentsflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class CaptureAdmissionServiceTests {

	private static final UUID MERCHANT_ID = UUID.randomUUID();
	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

	private final CaptureAdmissionRepository repository = mock(CaptureAdmissionRepository.class);
	private final CaptureRequestHasher requestHasher = mock(CaptureRequestHasher.class);
	private final CaptureAuthorizationValidityClient validityClient = mock(CaptureAuthorizationValidityClient.class);
	private final PaymentProperties properties = new PaymentProperties(MERCHANT_ID);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

	private final CaptureAdmissionService service =
			new CaptureAdmissionService(repository, properties, requestHasher, validityClient, clock);

	@Test
	void malformedPaymentIdIsRejected() {
		assertThatThrownBy(() -> service.admit("not-a-uuid", "key"))
				.isInstanceOf(InvalidPaymentIdException.class);
	}

	@Test
	void unknownPaymentIsRejected() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId)).thenReturn(Optional.empty());

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "key"))
				.isInstanceOf(PaymentNotFoundException.class);
		verify(validityClient, never()).isAuthorizationActive(any());
	}

	@Test
	void missingAuthorizationPspReferenceFailsClosed() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId))
				.thenReturn(Optional.of(new CaptureAdmissionRepository.CaptureDerivation(50_000L, "BRL", null)));

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "key"))
				.isInstanceOf(PspStatusUnavailableException.class);
		verify(validityClient, never()).isAuthorizationActive(any());
	}

	@Test
	void malformedAuthorizationPspReferenceFailsClosed() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId))
				.thenReturn(Optional.of(new CaptureAdmissionRepository.CaptureDerivation(50_000L, "BRL", "not-a-uuid")));

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "key"))
				.isInstanceOf(PspStatusUnavailableException.class);
		verify(validityClient, never()).isAuthorizationActive(any());
	}

	@Test
	void inactiveAuthorizationEvidenceFailsClosedWithoutTouchingTheRepositoryAdmission() {
		UUID paymentId = UUID.randomUUID();
		UUID authorizationPspOperationId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId)).thenReturn(Optional.of(
				new CaptureAdmissionRepository.CaptureDerivation(50_000L, "BRL", authorizationPspOperationId.toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash"))).thenReturn(Optional.empty());
		when(validityClient.isAuthorizationActive(authorizationPspOperationId)).thenReturn(false);

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "key"))
				.isInstanceOf(PspStatusUnavailableException.class);
		verify(repository, never()).admitCapture(any(), any(), any(), any());
	}

	@Test
	void existingAdmissionIsReturnedWithoutProbingThePsp() {
		UUID paymentId = UUID.randomUUID();
		UUID authorizationPspOperationId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId)).thenReturn(Optional.of(
				new CaptureAdmissionRepository.CaptureDerivation(50_000L, "BRL", authorizationPspOperationId.toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		CreatePaymentResponse existing = new CreatePaymentResponse(paymentId, UUID.randomUUID(), "CAPTURED", "SUCCEEDED");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash"))).thenReturn(Optional.of(existing));

		CreatePaymentResponse response = service.admit(paymentId.toString(), "capture-key");

		assertThat(response).isEqualTo(existing);
		verify(validityClient, never()).isAuthorizationActive(any());
		verify(repository, never()).admitCapture(any(), any(), any(), any());
	}

	@Test
	void conflictingPrecheckOutcomePropagatesWithoutProbingThePsp() {
		UUID paymentId = UUID.randomUUID();
		UUID authorizationPspOperationId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId)).thenReturn(Optional.of(
				new CaptureAdmissionRepository.CaptureDerivation(50_000L, "BRL", authorizationPspOperationId.toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash")))
				.thenThrow(new PaymentAlreadyCapturedException());

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "capture-key"))
				.isInstanceOf(PaymentAlreadyCapturedException.class);
		verify(validityClient, never()).isAuthorizationActive(any());
		verify(repository, never()).admitCapture(any(), any(), any(), any());
	}

	@Test
	void activeEvidenceDelegatesToRepositoryWithDerivedCommandAndHash() {
		UUID paymentId = UUID.randomUUID();
		UUID authorizationPspOperationId = UUID.randomUUID();
		when(repository.findCaptureDerivation(MERCHANT_ID, paymentId)).thenReturn(Optional.of(
				new CaptureAdmissionRepository.CaptureDerivation(50_000L, "BRL", authorizationPspOperationId.toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash"))).thenReturn(Optional.empty());
		when(validityClient.isAuthorizationActive(authorizationPspOperationId)).thenReturn(true);
		CreatePaymentResponse expected = new CreatePaymentResponse(paymentId, UUID.randomUUID(), "AUTHORIZED", "PENDING");
		when(repository.admitCapture(eq(MERCHANT_ID), any(), eq("hash"), eq(NOW))).thenReturn(expected);

		CreatePaymentResponse response = service.admit(paymentId.toString(), "capture-key");

		assertThat(response).isEqualTo(expected);
		verify(requestHasher).hash(new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "capture-key"));
		verify(repository).precheckAdmission(MERCHANT_ID, new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "capture-key"), "hash");
		verify(repository).admitCapture(MERCHANT_ID, new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "capture-key"), "hash", NOW);
	}
}
