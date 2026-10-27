package jhsoftware.pspsimulator;

import java.time.Instant;
import java.util.UUID;

record OperationDecisionResponse(
		UUID pspOperationId, String result, String reasonCode, String validity, Instant validityEvidenceAt) {
}
