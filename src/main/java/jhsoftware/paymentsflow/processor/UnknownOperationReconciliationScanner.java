package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.time.Duration;
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
class UnknownOperationReconciliationScanner {

	private static final Logger logger = LoggerFactory.getLogger(UnknownOperationReconciliationScanner.class);
	private static final int MAX_BACKOFF_SHIFT = 30;

	private final ReconciliationJobRepository repository;
	private final PspLookupClient lookupClient;
	private final ProcessorIdentity identity;
	private final ProcessorProperties properties;
	private final Clock clock;

	UnknownOperationReconciliationScanner(
			ReconciliationJobRepository repository,
			PspLookupClient lookupClient,
			ProcessorIdentity identity,
			ProcessorProperties properties,
			Clock clock) {
		this.repository = repository;
		this.lookupClient = lookupClient;
		this.identity = identity;
		this.properties = properties;
		this.clock = clock;
	}

	@Scheduled(fixedDelay = 5000, initialDelay = 5000)
	void scan() {
		UUID self = identity.id();
		Instant now = clock.instant();
		ProcessorProperties.Reconciliation reconciliation = properties.reconciliation();

		List<ReconciliationJobRepository.ReconciliationJobClaim> claims =
				repository.claimDueJobs(self, now, reconciliation.batchSize(), reconciliation.claimLease());

		for (ReconciliationJobRepository.ReconciliationJobClaim claim : claims) {
			resolveOrBackOff(claim, self);
		}
	}

	private void resolveOrBackOff(ReconciliationJobRepository.ReconciliationJobClaim claim, UUID self) {
		PspAuthorizationOutcome outcome = lookupClient.lookup(claim.operationId());
		Instant now = clock.instant();

		if (outcome.isDefinitive()) {
			boolean resolved = repository.resolveJob(claim.jobId(), self, claim.operationId(), outcome, UUID.randomUUID(), null, now);
			if (!resolved) {
				logger.warn("Reconciliation job {} could not be resolved because its lease was lost", claim.jobId());
			}
			return;
		}

		Instant nextCheckAt = now.plus(backoff(claim.attempts(), properties.reconciliation()));
		boolean backedOff = repository.bumpBackoff(claim.jobId(), self, outcome.reasonCode(), now, nextCheckAt);
		if (!backedOff) {
			logger.warn("Reconciliation job {} could not be backed off because its lease was lost", claim.jobId());
		}
	}

	private Duration backoff(int attempts, ProcessorProperties.Reconciliation reconciliation) {
		Duration initial = reconciliation.backoffInitial();
		int shift = Math.min(attempts, MAX_BACKOFF_SHIFT);
		Duration candidate = initial.multipliedBy(1L << shift);
		return candidate.compareTo(reconciliation.backoffMax()) > 0 ? reconciliation.backoffMax() : candidate;
	}
}
