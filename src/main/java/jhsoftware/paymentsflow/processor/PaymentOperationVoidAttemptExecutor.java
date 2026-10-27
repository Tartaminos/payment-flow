package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationVoidAttemptExecutor {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationVoidAttemptExecutor.class);
	private static final int MAX_ATTEMPTS = 3;
	private static final String OPERATION_TYPE = "VOID";

	private final PaymentOperationProcessingRepository repository;
	private final List<PspVoidClient> attemptClients;
	private final PspLookupClient lookupClient;
	private final ProcessorIdentity identity;
	private final ProcessorProperties properties;
	private final Clock clock;
	private final MeterRegistry meterRegistry;

	PaymentOperationVoidAttemptExecutor(
			PaymentOperationProcessingRepository repository,
			List<PspVoidClient> pspVoidAttemptClients,
			PspLookupClient lookupClient,
			ProcessorIdentity identity,
			ProcessorProperties properties,
			Clock clock,
			MeterRegistry meterRegistry) {
		if (pspVoidAttemptClients.size() != MAX_ATTEMPTS) {
			throw new IllegalArgumentException("Exactly 3 PSP void attempt clients are required");
		}
		this.repository = repository;
		this.attemptClients = pspVoidAttemptClients;
		this.lookupClient = lookupClient;
		this.identity = identity;
		this.properties = properties;
		this.clock = clock;
		this.meterRegistry = meterRegistry;
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

		Optional<UUID> authorizationPspOperationId = parseAuthorizationPspOperationId(context.authorizationPspReference());
		if (authorizationPspOperationId.isEmpty()) {
			logger.error(
					"Operation {} has no resolvable authorization PSP reference; committing as unknown for reconciliation",
					context.operationId());
			return repository.commitUnknownWithReconciliationJob(
					context.operationId(), self, "AUTHORIZATION_REFERENCE_UNAVAILABLE", correlationId, causationId, clock.instant());
		}

		int attemptsUsed = claim.attemptCount();
		String lastReasonCode = "PSP_ATTEMPTS_EXHAUSTED";

		if (claim.isTakeover(self)) {
			PspAuthorizationOutcome lookupOutcome = lookupClient.lookup(context.operationId());
			if (lookupOutcome.isDefinitive()) {
				return repository.commitOutcome(
						context.operationId(), self, lookupOutcome, correlationId, causationId, clock.instant());
			}
			lastReasonCode = lookupOutcome.reasonCode();
		}

		while (attemptsUsed < MAX_ATTEMPTS) {
			Optional<Integer> reserved = repository.reserveAttempt(context.operationId(), self, clock.instant(), leaseDuration);
			if (reserved.isEmpty()) {
				break;
			}

			int attemptNumber = reserved.get();
			Timer.Sample sample = Timer.start();
			PspAuthorizationOutcome outcome;
			try {
				outcome = attemptClients.get(attemptNumber - 1).voidOperation(context, authorizationPspOperationId.get());
			}
			finally {
				sample.stop(Timer.builder("paymentflow.psp.call.duration")
						.tag("operation_type", OPERATION_TYPE)
						.register(meterRegistry));
			}
			Counter.builder("paymentflow.psp.outcomes")
					.tag("operation_type", OPERATION_TYPE)
					.tag("outcome_type", outcome.type().name())
					.register(meterRegistry)
					.increment();
			if (outcome.isDefinitive()) {
				return repository.commitOutcome(
						context.operationId(), self, outcome, correlationId, causationId, clock.instant());
			}

			attemptsUsed = attemptNumber;
			lastReasonCode = outcome.reasonCode();
			if (attemptsUsed < MAX_ATTEMPTS) {
				sleep(gapAfter(attemptNumber));
			}
		}

		return repository.commitUnknownWithReconciliationJob(
				context.operationId(), self, lastReasonCode, correlationId, causationId, clock.instant());
	}

	private Optional<UUID> parseAuthorizationPspOperationId(String authorizationPspReference) {
		if (authorizationPspReference == null) {
			return Optional.empty();
		}
		try {
			return Optional.of(UUID.fromString(authorizationPspReference));
		}
		catch (IllegalArgumentException exception) {
			return Optional.empty();
		}
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
