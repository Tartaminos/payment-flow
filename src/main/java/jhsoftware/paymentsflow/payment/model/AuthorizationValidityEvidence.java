package jhsoftware.paymentsflow.payment.model;

import java.time.Instant;

public record AuthorizationValidityEvidence(AuthorizationValidityStatus status, Instant validityEvidenceAt) {

	public static AuthorizationValidityEvidence unknown() {
		return new AuthorizationValidityEvidence(AuthorizationValidityStatus.UNKNOWN, null);
	}
}
