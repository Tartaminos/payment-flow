package jhsoftware.paymentsflow.processor.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class OutcomeMappingTests {

	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	@Test
	void authorizeSucceededMapsToAuthorizedPayment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"AUTHORIZE", PspAuthorizationOutcome.succeeded("ref"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("SUCCEEDED");
		assertThat(mapping.newPaymentStatus()).isEqualTo("AUTHORIZED");
		assertThat(mapping.ackEligible()).isTrue();
	}

	@Test
	void authorizeRejectedMapsToDeclinedPayment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"AUTHORIZE", PspAuthorizationOutcome.rejected("ref", "CARD_DECLINED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("REJECTED");
		assertThat(mapping.newPaymentStatus()).isEqualTo("DECLINED");
		assertThat(mapping.errorCode()).isEqualTo("CARD_DECLINED");
	}

	@Test
	void authorizeFailedLeavesPaymentStatusUnchanged() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"AUTHORIZE", PspAuthorizationOutcome.failed("ref", "NON_EXECUTION_CONFIRMED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("FAILED");
		assertThat(mapping.newPaymentStatus()).isNull();
	}

	@Test
	void authorizeUnknownLeavesPaymentStatusUnchangedAndWithholdsAcknowledgment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"AUTHORIZE", PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("UNKNOWN");
		assertThat(mapping.newPaymentStatus()).isNull();
		assertThat(mapping.ackEligible()).isFalse();
	}

	@Test
	void captureSucceededMapsToCapturedPayment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"CAPTURE", PspAuthorizationOutcome.succeeded("ref"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("SUCCEEDED");
		assertThat(mapping.newPaymentStatus()).isEqualTo("CAPTURED");
	}

	@Test
	void captureOrdinaryRejectionLeavesPaymentStatusUnchanged() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"CAPTURE", PspAuthorizationOutcome.rejected("ref", "NON_EXECUTION_CONFIRMED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("REJECTED");
		assertThat(mapping.newPaymentStatus()).isNull();
	}

	@Test
	void captureRejectionWithAuthorizationExpiredReasonTransitionsPaymentToExpired() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"CAPTURE", PspAuthorizationOutcome.rejected("ref", "AUTHORIZATION_EXPIRED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("REJECTED");
		assertThat(mapping.newPaymentStatus()).isEqualTo("EXPIRED");
	}

	@Test
	void captureFailedLeavesPaymentStatusUnchanged() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"CAPTURE", PspAuthorizationOutcome.failed("ref", "NON_EXECUTION_CONFIRMED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("FAILED");
		assertThat(mapping.newPaymentStatus()).isNull();
	}

	@Test
	void captureUnknownLeavesPaymentStatusUnchangedAndWithholdsAcknowledgment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"CAPTURE", PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("UNKNOWN");
		assertThat(mapping.newPaymentStatus()).isNull();
		assertThat(mapping.ackEligible()).isFalse();
	}

	@Test
	void voidSucceededMapsToCanceledPayment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"VOID", PspAuthorizationOutcome.succeeded("ref"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("SUCCEEDED");
		assertThat(mapping.newPaymentStatus()).isEqualTo("CANCELED");
		assertThat(mapping.ackEligible()).isTrue();
	}

	@Test
	void voidRejectionLeavesPaymentStatusUnchanged() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"VOID", PspAuthorizationOutcome.rejected("ref", "NON_EXECUTION_CONFIRMED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("REJECTED");
		assertThat(mapping.newPaymentStatus()).isNull();
	}

	@Test
	void voidRejectionWithAuthorizationExpiredReasonNeverTransitionsPaymentToExpired() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"VOID", PspAuthorizationOutcome.rejected("ref", "AUTHORIZATION_EXPIRED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("REJECTED");
		assertThat(mapping.newPaymentStatus()).isNull();
	}

	@Test
	void voidFailedLeavesPaymentStatusUnchanged() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"VOID", PspAuthorizationOutcome.failed("ref", "NON_EXECUTION_CONFIRMED"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("FAILED");
		assertThat(mapping.newPaymentStatus()).isNull();
	}

	@Test
	void voidUnknownLeavesPaymentStatusUnchangedAndWithholdsAcknowledgment() {
		OutcomeMapping mapping = OutcomeMapping.forOutcome(
				"VOID", PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"), NOW);

		assertThat(mapping.newOperationStatus()).isEqualTo("UNKNOWN");
		assertThat(mapping.newPaymentStatus()).isNull();
		assertThat(mapping.ackEligible()).isFalse();
	}
}
