package jhsoftware.paymentsflow.processor.dto;

import java.time.Instant;
import java.util.UUID;

public record PspAuthorizeResponseDto(UUID pspOperationId, String result, String reasonCode, String validity, Instant validityEvidenceAt) {
}
