package jhsoftware.paymentsflow.processor.exception;

import java.util.UUID;

public final class LostScheduledRetryClaimException extends RuntimeException {

	private final UUID retryId;

	public LostScheduledRetryClaimException(UUID retryId) {
		super("Scheduled retry " + retryId + " claim was lost before dispatch could be recorded");
		this.retryId = retryId;
	}

	UUID retryId() {
		return retryId;
	}
}
