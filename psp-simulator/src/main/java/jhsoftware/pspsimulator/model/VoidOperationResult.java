package jhsoftware.pspsimulator.model;

import jhsoftware.pspsimulator.dto.OperationDecisionResponseDto;

public record VoidOperationResult(OperationDecisionResponseDto decision, boolean stillProcessing) {
}
