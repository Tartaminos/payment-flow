package jhsoftware.paymentsflow.processor;

import java.time.Instant;
import java.util.UUID;

record ScheduledRetry(
		UUID retryId,
		int retryNumber,
		Instant scheduledAt,
		Instant nextEligibleAt) {
}
