package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationAttemptExecutor {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationAttemptExecutor.class);
	private static final int MAX_ATTEMPTS = 3;

	private final PaymentOperationProcessingRepository repository;
	private final List<PspAuthorizationClient> attemptClients;
	private final PspLookupClient lookupClient;
	private final ProcessorIdentity identity;
	private final ProcessorProperties properties;
	private final Clock clock;

	PaymentOperationAttemptExecutor(
			PaymentOperationProcessingRepository repository,
			List<PspAuthorizationClient> pspAuthorizationAttemptClients,
			PspLookupClient lookupClient,
			ProcessorIdentity identity,
			ProcessorProperties properties,
			Clock clock) {
		if (pspAuthorizationAttemptClients.size() != MAX_ATTEMPTS) {
			throw new IllegalArgumentException("Exactly 3 PSP authorization attempt clients are required");
		}
		this.repository = repository;
		this.attemptClients = pspAuthorizationAttemptClients;
		this.lookupClient = lookupClient;
		this.identity = identity;
		this.properties = properties;
		this.clock = clock;
	}

	boolean execute(PaymentOperationContext context, UUID correlationId, UUID causationId) {
		UUID self = identity.id();
		Duration leaseDuration = properties.operationLeaseDuration();

		PaymentOperationProcessingRepository.LeaseClaim claim =
				repository.claimOrRenewLease(context.operationId(), self, clock.instant(), leaseDuration);

		if (claim.status() == PaymentOperationProcessingRepository.LeaseClaim.Status.NOT_FOUND) {
			return false;
		}
		if (claim.status() == PaymentOperationProcessingRepository.LeaseClaim.Status.FINALIZED) {
			return true;
		}
		if (claim.status() == PaymentOperationProcessingRepository.LeaseClaim.Status.DENIED) {
			logger.warn(
					"Operation {} processing lease is held by another owner; withholding acknowledgment",
					context.operationId());
			return false;
		}

		int attemptsUsed = claim.attemptCount();
		String lastReasonCode = "PSP_ATTEMPTS_EXHAUSTED";

		if (claim.isTakeover(self)) {
			PspAuthorizationOutcome lookupOutcome = lookupClient.lookup(context.operationId());
			if (lookupOutcome.isDefinitive()) {
				return repository.commitOutcome(context.operationId(), lookupOutcome, correlationId, causationId, clock.instant());
			}
			lastReasonCode = lookupOutcome.reasonCode();
		}

		while (attemptsUsed < MAX_ATTEMPTS) {
			Optional<Integer> reserved = repository.reserveAttempt(context.operationId(), self, clock.instant(), leaseDuration);
			if (reserved.isEmpty()) {
				break;
			}

			int attemptNumber = reserved.get();
			PspAuthorizationOutcome outcome = attemptClients.get(attemptNumber - 1).authorize(context);
			if (outcome.isDefinitive()) {
				return repository.commitOutcome(context.operationId(), outcome, correlationId, causationId, clock.instant());
			}

			attemptsUsed = attemptNumber;
			lastReasonCode = outcome.reasonCode();
			if (attemptsUsed < MAX_ATTEMPTS) {
				sleep(gapAfter(attemptNumber));
			}
		}

		return repository.commitUnknownWithReconciliationJob(
				context.operationId(), lastReasonCode, correlationId, causationId, clock.instant());
	}

	private Duration gapAfter(int attemptNumber) {
		return attemptNumber == 1 ? properties.retryGapAfterFirstAttempt() : properties.retryGapAfterSecondAttempt();
	}

	private void sleep(Duration duration) {
		try {
			Thread.sleep(duration.toMillis());
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}
}
