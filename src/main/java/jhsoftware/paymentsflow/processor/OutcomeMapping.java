package jhsoftware.paymentsflow.processor;

import java.time.Instant;

record OutcomeMapping(
		String newOperationStatus,
		String newPaymentStatus,
		String errorCode,
		Instant completedAt,
		Instant unknownSince,
		boolean ackEligible) {

	static OutcomeMapping forOutcome(PspAuthorizationOutcome outcome, Instant now) {
		return switch (outcome.type()) {
			case SUCCEEDED -> new OutcomeMapping("SUCCEEDED", "AUTHORIZED", null, now, null, true);
			case REJECTED -> new OutcomeMapping("REJECTED", "DECLINED", outcome.reasonCode(), now, null, true);
			case FAILED -> new OutcomeMapping("FAILED", null, outcome.reasonCode(), now, null, true);
			case UNKNOWN -> new OutcomeMapping("UNKNOWN", null, outcome.reasonCode(), null, now, false);
		};
	}
}
