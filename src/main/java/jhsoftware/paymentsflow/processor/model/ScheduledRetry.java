package jhsoftware.paymentsflow.processor.model;

import java.time.Instant;
import java.util.UUID;

public record ScheduledRetry(
		UUID retryId,
		int retryNumber,
		Instant scheduledAt,
		Instant nextEligibleAt) {
}
