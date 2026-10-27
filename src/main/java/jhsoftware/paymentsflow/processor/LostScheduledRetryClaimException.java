package jhsoftware.paymentsflow.processor;

import java.util.UUID;

final class LostScheduledRetryClaimException extends RuntimeException {

	private final UUID retryId;

	LostScheduledRetryClaimException(UUID retryId) {
		super("Scheduled retry " + retryId + " claim was lost before dispatch could be recorded");
		this.retryId = retryId;
	}

	UUID retryId() {
		return retryId;
	}
}
