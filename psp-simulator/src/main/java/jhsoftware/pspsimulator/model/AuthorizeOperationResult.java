package jhsoftware.pspsimulator.model;

import jhsoftware.pspsimulator.dto.OperationDecisionResponseDto;

public record AuthorizeOperationResult(OperationDecisionResponseDto decision, boolean stillProcessing) {
}
