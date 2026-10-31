package jhsoftware.pspsimulator.dto;

import java.time.Instant;
import java.util.UUID;

public record OperationDecisionResponseDto(
		UUID pspOperationId, String result, String reasonCode, String validity, Instant validityEvidenceAt) {
}
