package jhsoftware.pspsimulator.model;

import jhsoftware.pspsimulator.dto.OperationDecisionResponseDto;

public record CaptureOperationResult(OperationDecisionResponseDto decision, boolean stillProcessing) {
}
