package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class ScheduledRetryDispatcher {

	private static final Logger logger = LoggerFactory.getLogger(ScheduledRetryDispatcher.class);

	private final ScheduledRetryRepository repository;
	private final ProcessorProperties properties;
	private final Clock clock;
	private final UUID workerId;

	ScheduledRetryDispatcher(ScheduledRetryRepository repository, ProcessorProperties properties, Clock clock) {
		this.repository = repository;
		this.properties = properties;
		this.clock = clock;
		this.workerId = UUID.randomUUID();
	}

	@Scheduled(
			fixedDelayString = "${paymentflow.processor.dispatcher.poll-interval:2s}",
			initialDelayString = "${paymentflow.processor.dispatcher.poll-interval:2s}")
	void dispatchDue() {
		Instant now = clock.instant();
		Instant leaseExpiresAt = now.plus(properties.dispatcher().claimLease());

		List<DueScheduledRetry> claimed = repository.claimDue(
				properties.dispatcher().batchMax(), workerId, now, leaseExpiresAt);

		for (DueScheduledRetry retry : claimed) {
			try {
				repository.dispatch(retry, workerId, clock.instant());
			}
			catch (LostScheduledRetryClaimException exception) {
				logger.warn(exception.getMessage());
			}
			catch (Exception exception) {
				logger.warn(
						"Scheduled retry {} dispatch failed ({})",
						retry.retryId(),
						rootCause(exception).getClass().getSimpleName());
			}
		}
	}

	private Throwable rootCause(Throwable failure) {
		Throwable cause = failure;
		while (cause.getCause() != null && cause.getCause() != cause) {
			cause = cause.getCause();
		}
		return cause;
	}
}
