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

class VoidAdmissionServiceTests {

	private static final UUID MERCHANT_ID = UUID.randomUUID();
	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

	private final VoidAdmissionRepository repository = mock(VoidAdmissionRepository.class);
	private final VoidRequestHasher requestHasher = mock(VoidRequestHasher.class);
	private final PaymentProperties properties = new PaymentProperties(MERCHANT_ID);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

	private final VoidAdmissionService service =
			new VoidAdmissionService(repository, properties, requestHasher, clock);

	@Test
	void malformedPaymentIdIsRejected() {
		assertThatThrownBy(() -> service.admit("not-a-uuid", "key"))
				.isInstanceOf(InvalidPaymentIdException.class);
	}

	@Test
	void unknownPaymentIsRejected() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findVoidDerivation(MERCHANT_ID, paymentId)).thenReturn(Optional.empty());

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "key"))
				.isInstanceOf(PaymentNotFoundException.class);
		verify(repository, never()).admitVoid(any(), any(), any(), any());
	}

	@Test
	void existingAdmissionIsReturnedWithoutCallingAdmitVoid() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findVoidDerivation(MERCHANT_ID, paymentId)).thenReturn(
				Optional.of(new VoidAdmissionRepository.VoidDerivation(50_000L, "BRL", UUID.randomUUID().toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		CreatePaymentResponse existing = new CreatePaymentResponse(paymentId, UUID.randomUUID(), "CANCELED", "SUCCEEDED");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash"))).thenReturn(Optional.of(existing));

		CreatePaymentResponse response = service.admit(paymentId.toString(), "void-key");

		assertThat(response).isEqualTo(existing);
		verify(repository, never()).admitVoid(any(), any(), any(), any());
	}

	@Test
	void conflictingPrecheckOutcomePropagatesWithoutCallingAdmitVoid() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findVoidDerivation(MERCHANT_ID, paymentId)).thenReturn(
				Optional.of(new VoidAdmissionRepository.VoidDerivation(50_000L, "BRL", UUID.randomUUID().toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash")))
				.thenThrow(new PaymentAlreadyCapturedException());

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "void-key"))
				.isInstanceOf(PaymentAlreadyCapturedException.class);
		verify(repository, never()).admitVoid(any(), any(), any(), any());
	}

	@Test
	void eligiblePaymentDelegatesDirectlyToRepositoryWithoutAnyPspCall() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findVoidDerivation(MERCHANT_ID, paymentId)).thenReturn(
				Optional.of(new VoidAdmissionRepository.VoidDerivation(50_000L, "BRL", UUID.randomUUID().toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash"))).thenReturn(Optional.empty());
		CreatePaymentResponse expected = new CreatePaymentResponse(paymentId, UUID.randomUUID(), "AUTHORIZED", "PENDING");
		when(repository.admitVoid(eq(MERCHANT_ID), any(), eq("hash"), eq(NOW))).thenReturn(expected);

		CreatePaymentResponse response = service.admit(paymentId.toString(), "void-key");

		assertThat(response).isEqualTo(expected);
		verify(requestHasher).hash(new VoidAdmissionCommand(paymentId, 50_000L, "BRL", "void-key"));
		verify(repository).precheckAdmission(MERCHANT_ID, new VoidAdmissionCommand(paymentId, 50_000L, "BRL", "void-key"), "hash");
		verify(repository).admitVoid(MERCHANT_ID, new VoidAdmissionCommand(paymentId, 50_000L, "BRL", "void-key"), "hash", NOW);
	}

	@Test
	void repositoryExpiredRejectionPropagates() {
		UUID paymentId = UUID.randomUUID();
		when(repository.findVoidDerivation(MERCHANT_ID, paymentId)).thenReturn(
				Optional.of(new VoidAdmissionRepository.VoidDerivation(50_000L, "BRL", UUID.randomUUID().toString())));
		when(requestHasher.hash(any())).thenReturn("hash");
		when(repository.precheckAdmission(eq(MERCHANT_ID), any(), eq("hash"))).thenReturn(Optional.empty());
		when(repository.admitVoid(eq(MERCHANT_ID), any(), eq("hash"), eq(NOW)))
				.thenThrow(new AuthorizationExpiredException());

		assertThatThrownBy(() -> service.admit(paymentId.toString(), "void-key"))
				.isInstanceOf(AuthorizationExpiredException.class);
	}
}
