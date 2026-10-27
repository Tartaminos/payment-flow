package jhsoftware.paymentsflow.processor;

import java.time.Instant;

record OutcomeMapping(
		String newOperationStatus,
		String newPaymentStatus,
		String errorCode,
		Instant completedAt,
		Instant unknownSince,
		boolean ackEligible) {

	private static final String AUTHORIZATION_EXPIRED_REASON = "AUTHORIZATION_EXPIRED";

	static OutcomeMapping forOutcome(String operationType, PspAuthorizationOutcome outcome, Instant now) {
		if ("CAPTURE".equals(operationType)) {
			return forCaptureOutcome(outcome, now);
		}
		if ("VOID".equals(operationType)) {
			return forVoidOutcome(outcome, now);
		}
		return forAuthorizeOutcome(outcome, now);
	}

	private static OutcomeMapping forAuthorizeOutcome(PspAuthorizationOutcome outcome, Instant now) {
		return switch (outcome.type()) {
			case SUCCEEDED -> new OutcomeMapping("SUCCEEDED", "AUTHORIZED", null, now, null, true);
			case REJECTED -> new OutcomeMapping("REJECTED", "DECLINED", outcome.reasonCode(), now, null, true);
			case FAILED -> new OutcomeMapping("FAILED", null, outcome.reasonCode(), now, null, true);
			case UNKNOWN -> new OutcomeMapping("UNKNOWN", null, outcome.reasonCode(), null, now, false);
		};
	}

	private static OutcomeMapping forCaptureOutcome(PspAuthorizationOutcome outcome, Instant now) {
		return switch (outcome.type()) {
			case SUCCEEDED -> new OutcomeMapping("SUCCEEDED", "CAPTURED", null, now, null, true);
			case REJECTED -> new OutcomeMapping(
					"REJECTED",
					AUTHORIZATION_EXPIRED_REASON.equals(outcome.reasonCode()) ? "EXPIRED" : null,
					outcome.reasonCode(),
					now,
					null,
					true);
			case FAILED -> new OutcomeMapping("FAILED", null, outcome.reasonCode(), now, null, true);
			case UNKNOWN -> new OutcomeMapping("UNKNOWN", null, outcome.reasonCode(), null, now, false);
		};
	}

	private static OutcomeMapping forVoidOutcome(PspAuthorizationOutcome outcome, Instant now) {
		return switch (outcome.type()) {
			case SUCCEEDED -> new OutcomeMapping("SUCCEEDED", "CANCELED", null, now, null, true);
			case REJECTED -> new OutcomeMapping("REJECTED", null, outcome.reasonCode(), now, null, true);
			case FAILED -> new OutcomeMapping("FAILED", null, outcome.reasonCode(), now, null, true);
			case UNKNOWN -> new OutcomeMapping("UNKNOWN", null, outcome.reasonCode(), null, now, false);
		};
	}
}
