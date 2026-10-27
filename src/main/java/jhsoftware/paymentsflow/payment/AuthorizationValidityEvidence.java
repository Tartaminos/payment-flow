package jhsoftware.paymentsflow.payment;

import java.time.Instant;

record AuthorizationValidityEvidence(AuthorizationValidityStatus status, Instant validityEvidenceAt) {

	static AuthorizationValidityEvidence unknown() {
		return new AuthorizationValidityEvidence(AuthorizationValidityStatus.UNKNOWN, null);
	}
}
