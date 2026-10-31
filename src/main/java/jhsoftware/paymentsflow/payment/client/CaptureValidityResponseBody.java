package jhsoftware.paymentsflow.payment.client;

import java.time.Instant;
import java.util.UUID;

record CaptureValidityResponseBody(UUID pspOperationId, String result, String reasonCode, String validity, Instant validityEvidenceAt) {
}
