package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class UnknownOperationReconciliationScanner {

	private static final Logger logger = LoggerFactory.getLogger(UnknownOperationReconciliationScanner.class);
	private static final int MAX_BACKOFF_SHIFT = 30;
	private static final int MAX_CONCURRENT_LOOKUPS = 4;
	private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
	private static final String CONTRADICTION_REASON = "PSP_EVIDENCE_CONTRADICTION";
	private static final String UNKNOWN_24H_REASON = "PSP_UNKNOWN_24H";
	private static final Duration ESCALATION_THRESHOLD = Duration.ofHours(24);

	private final ReconciliationJobRepository repository;
	private final ReconciliationEscalationRepository escalationRepository;
	private final PspLookupClient lookupClient;
	private final ProcessorIdentity identity;
	private final ProcessorProperties properties;
	private final Clock clock;
	private final ReconciliationScannerHealth health;
	private final ExecutorService lookupExecutor;
	private final MeterRegistry meterRegistry;

	UnknownOperationReconciliationScanner(
			ReconciliationJobRepository repository,
			ReconciliationEscalationRepository escalationRepository,
			PspLookupClient lookupClient,
			ProcessorIdentity identity,
			ProcessorProperties properties,
			Clock clock,
			ReconciliationScannerHealth health,
			MeterRegistry meterRegistry) {
		this.repository = repository;
		this.escalationRepository = escalationRepository;
		this.lookupClient = lookupClient;
		this.identity = identity;
		this.properties = properties;
		this.clock = clock;
		this.health = health;
		this.lookupExecutor = Executors.newFixedThreadPool(MAX_CONCURRENT_LOOKUPS);
		this.meterRegistry = meterRegistry;
	}

	@Scheduled(fixedDelay = 5000, initialDelay = 5000)
	void scan() {
		UUID self = identity.id();
		Instant now = clock.instant();
		ProcessorProperties.Reconciliation reconciliation = properties.reconciliation();

		List<ReconciliationJobRepository.ReconciliationJobClaim> claims =
				repository.claimDueJobs(self, now, reconciliation.batchSize(), reconciliation.claimLease());

		for (ReconciliationJobRepository.ReconciliationJobClaim claim : claims) {
			Counter.builder("paymentflow.reconciliation.due").register(meterRegistry).increment();
			try {
				CorrelationContext.put(CorrelationContext.OPERATION_ID, claim.operationId());
				escalateIfPastThreshold(claim, now);
			}
			finally {
				CorrelationContext.clear();
			}
		}

		List<Future<?>> futures = new ArrayList<>(claims.size());
		for (ReconciliationJobRepository.ReconciliationJobClaim claim : claims) {
			futures.add(lookupExecutor.submit(() -> resolveOrBackOff(claim, self)));
		}
		for (Future<?> future : futures) {
			awaitCompletion(future);
		}

		health.recordHeartbeat(now);
	}

	private void escalateIfPastThreshold(ReconciliationJobRepository.ReconciliationJobClaim claim, Instant now) {
		if (now.isBefore(claim.unknownSince().plus(ESCALATION_THRESHOLD))) {
			return;
		}
		boolean created = escalationRepository.ensureEscalation(claim.operationId(), claim.jobId(), UNKNOWN_24H_REASON, now);
		if (created) {
			Counter.builder("paymentflow.reconciliation.escalated")
					.tag("reason", UNKNOWN_24H_REASON)
					.register(meterRegistry)
					.increment();
			logger.warn("Operation {} escalated after remaining UNKNOWN for 24 hours", claim.operationId());
		}
	}

	private void awaitCompletion(Future<?> future) {
		try {
			future.get();
		}
		catch (ExecutionException exception) {
			logger.warn("Reconciliation lookup task failed", exception.getCause());
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}

	private void resolveOrBackOff(ReconciliationJobRepository.ReconciliationJobClaim claim, UUID self) {
		try {
			CorrelationContext.put(CorrelationContext.OPERATION_ID, claim.operationId());
			resolveOrBackOffInternal(claim, self);
		}
		finally {
			CorrelationContext.clear();
		}
	}

	private void resolveOrBackOffInternal(ReconciliationJobRepository.ReconciliationJobClaim claim, UUID self) {
		PspAuthorizationOutcome outcome = lookupClient.lookup(claim.operationId());
		Instant now = clock.instant();

		if (outcome.isDefinitive()) {
			ReconciliationJobRepository.ResolutionOutcome resolution =
					repository.resolveJob(claim.jobId(), self, claim.operationId(), outcome, UUID.randomUUID(), null, now);
			switch (resolution) {
				case LEASE_LOST ->
						logger.warn("Reconciliation job {} could not be resolved because its lease was lost", claim.jobId());
				case CONTRADICTION -> {
					Instant contradictionNextCheckAt = now.plus(backoff(claim.attempts(), properties.reconciliation()));
					boolean flagged = repository.flagContradiction(
							claim.jobId(), self, claim.operationId(), now, contradictionNextCheckAt, CONTRADICTION_REASON);
					if (!flagged) {
						logger.warn("Reconciliation job {} could not be flagged because its lease was lost", claim.jobId());
					}
				}
				case RESOLVED -> { }
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

	@PreDestroy
	void shutdown() {
		lookupExecutor.shutdown();
		try {
			if (!lookupExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				lookupExecutor.shutdownNow();
			}
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			lookupExecutor.shutdownNow();
		}
	}
}
