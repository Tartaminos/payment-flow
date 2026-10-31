package jhsoftware.paymentsflow.processor.service;

import jhsoftware.paymentsflow.processor.config.ProcessorProperties;
import jhsoftware.paymentsflow.processor.exception.LostScheduledRetryClaimException;
import jhsoftware.paymentsflow.processor.model.DueScheduledRetry;
import jhsoftware.paymentsflow.processor.repository.ScheduledRetryRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class ScheduledRetryDispatcher {

	private static final Logger logger = LoggerFactory.getLogger(ScheduledRetryDispatcher.class);

	private final ScheduledRetryRepository repository;
	private final ProcessorProperties properties;
	private final Clock clock;
	private final UUID workerId;
	private final AtomicLong retryBacklog = new AtomicLong();
	private final AtomicLong oldestRetryPendingAgeSeconds = new AtomicLong();

	ScheduledRetryDispatcher(
			ScheduledRetryRepository repository,
			ProcessorProperties properties,
			Clock clock,
			MeterRegistry meterRegistry) {
		this.repository = repository;
		this.properties = properties;
		this.clock = clock;
		this.workerId = UUID.randomUUID();
		meterRegistry.gauge("paymentflow.retry.backlog", retryBacklog);
		meterRegistry.gauge("paymentflow.retry.oldest.pending.seconds", oldestRetryPendingAgeSeconds);
	}

	@Scheduled(
			fixedDelayString = "${paymentflow.processor.dispatcher.poll-interval:2s}",
			initialDelayString = "${paymentflow.processor.dispatcher.poll-interval:2s}")
	void dispatchDue() {
		Instant now = clock.instant();
		Instant leaseExpiresAt = now.plus(properties.dispatcher().claimLease());

		ScheduledRetryRepository.BacklogStats backlogStats = repository.backlogStats(now);
		retryBacklog.set(backlogStats.backlogCount());
		oldestRetryPendingAgeSeconds.set(backlogStats.oldestPendingAgeSeconds());

		List<DueScheduledRetry> claimed = repository.claimDue(
				properties.dispatcher().batchMax(), workerId, now, leaseExpiresAt);

		for (DueScheduledRetry retry : claimed) {
			try {
				CorrelationContext.put(CorrelationContext.RETRY_ID, retry.retryId());
				CorrelationContext.put(CorrelationContext.CORRELATION_ID, retry.correlationId());
				CorrelationContext.put(CorrelationContext.OPERATION_ID, retry.operationId());
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
			finally {
				CorrelationContext.clear();
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
