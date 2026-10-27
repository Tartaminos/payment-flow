package jhsoftware.paymentsflow.retention;

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
@ConditionalOnProperty(name = "paymentflow.retention.cleanup.enabled", havingValue = "true")
class RetentionCleanupJob {

	private static final Logger logger = LoggerFactory.getLogger(RetentionCleanupJob.class);

	private final RetentionCleanupRepository repository;
	private final RetentionCleanupProperties properties;
	private final Clock clock;

	RetentionCleanupJob(RetentionCleanupRepository repository, RetentionCleanupProperties properties, Clock clock) {
		this.repository = repository;
		this.properties = properties;
		this.clock = clock;
	}

	@Scheduled(cron = "${paymentflow.retention.cleanup.cron:0 0 3 1 * *}")
	void cleanup() {
		Instant now = clock.instant();
		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(now, properties.cleanup().financialRetention());

		if (properties.cleanup().dryRun()) {
			logger.info("Retention cleanup dry-run found {} candidate payment(s) for purge", candidates.size());
			return;
		}

		int deleted = repository.deleteCleanupCandidates(candidates);
		logger.info(
				"Retention cleanup purged audit/outbox/reconciliation records for {} candidate payment(s) ({} audit rows deleted)",
				candidates.size(), deleted);
	}
}
