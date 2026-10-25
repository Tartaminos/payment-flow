package jhsoftware.pspsimulator;

import java.util.UUID;

record OperationDecisionResponse(UUID pspOperationId, String result, String reasonCode) {
}
