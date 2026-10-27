package jhsoftware.paymentsflow.processor;

import java.time.Instant;
import java.util.UUID;

record PspAuthorizeResponseBody(UUID pspOperationId, String result, String reasonCode, String validity, Instant validityEvidenceAt) {
}
