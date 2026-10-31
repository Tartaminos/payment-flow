package jhsoftware.paymentsflow.processor.config;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.processor")
public record ProcessorProperties(
		URI pspBaseUrl,
		Duration pspConnectTimeout,
		List<Duration> pspAttemptReadTimeouts,
		Duration pspLookupReadTimeout,
		Duration operationLeaseDuration,
		Duration retryGapAfterFirstAttempt,
		Duration retryGapAfterSecondAttempt,
		Reconciliation reconciliation,
		Workers workers,
		Dispatcher dispatcher) {

	public ProcessorProperties {
		if (pspBaseUrl == null) {
			throw new IllegalArgumentException("paymentflow.processor.psp-base-url is required");
		}
		if (pspConnectTimeout == null || pspConnectTimeout.isNegative() || pspConnectTimeout.isZero()) {
			throw new IllegalArgumentException("paymentflow.processor.psp-connect-timeout must be positive");
		}
		if (pspAttemptReadTimeouts == null || pspAttemptReadTimeouts.size() != 3) {
			throw new IllegalArgumentException("paymentflow.processor.psp-attempt-read-timeouts must list exactly 3 timeouts");
		}
		for (Duration timeout : pspAttemptReadTimeouts) {
			if (timeout == null || timeout.isNegative() || timeout.isZero()) {
				throw new IllegalArgumentException("paymentflow.processor.psp-attempt-read-timeouts entries must be positive");
			}
		}
		if (pspLookupReadTimeout == null || pspLookupReadTimeout.isNegative() || pspLookupReadTimeout.isZero()) {
			throw new IllegalArgumentException("paymentflow.processor.psp-lookup-read-timeout must be positive");
		}
		if (operationLeaseDuration == null || operationLeaseDuration.isNegative() || operationLeaseDuration.isZero()) {
			throw new IllegalArgumentException("paymentflow.processor.operation-lease-duration must be positive");
		}
		if (retryGapAfterFirstAttempt == null || retryGapAfterFirstAttempt.isNegative()) {
			throw new IllegalArgumentException("paymentflow.processor.retry-gap-after-first-attempt must not be negative");
		}
		if (retryGapAfterSecondAttempt == null || retryGapAfterSecondAttempt.isNegative()) {
			throw new IllegalArgumentException("paymentflow.processor.retry-gap-after-second-attempt must not be negative");
		}
		if (reconciliation == null) {
			throw new IllegalArgumentException("paymentflow.processor.reconciliation is required");
		}
		if (workers == null) {
			throw new IllegalArgumentException("paymentflow.processor.workers is required");
		}
		if (dispatcher == null) {
			throw new IllegalArgumentException("paymentflow.processor.dispatcher is required");
		}
	}

	public record Reconciliation(Duration pollInterval, int batchSize, Duration claimLease, Duration backoffInitial, Duration backoffMax) {

		public Reconciliation {
			if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
				throw new IllegalArgumentException("paymentflow.processor.reconciliation.poll-interval must be positive");
			}
			if (batchSize < 1) {
				throw new IllegalArgumentException("paymentflow.processor.reconciliation.batch-size must be positive");
			}
			if (claimLease == null || claimLease.isNegative() || claimLease.isZero()) {
				throw new IllegalArgumentException("paymentflow.processor.reconciliation.claim-lease must be positive");
			}
			if (backoffInitial == null || backoffInitial.isNegative() || backoffInitial.isZero()) {
				throw new IllegalArgumentException("paymentflow.processor.reconciliation.backoff-initial must be positive");
			}
			if (backoffMax == null || backoffMax.compareTo(backoffInitial) < 0) {
				throw new IllegalArgumentException("paymentflow.processor.reconciliation.backoff-max must be at least the initial backoff");
			}
		}
	}

	public record Workers(int globalCapacity, int perPartitionCapacity, int perPaymentWaitingCapacity, int poolSize) {

		public Workers {
			if (globalCapacity < 1) {
				throw new IllegalArgumentException("paymentflow.processor.workers.global-capacity must be positive");
			}
			if (perPartitionCapacity < 1) {
				throw new IllegalArgumentException("paymentflow.processor.workers.per-partition-capacity must be positive");
			}
			if (perPaymentWaitingCapacity < 1) {
				throw new IllegalArgumentException("paymentflow.processor.workers.per-payment-waiting-capacity must be positive");
			}
			if (poolSize < 1) {
				throw new IllegalArgumentException("paymentflow.processor.workers.pool-size must be positive");
			}
		}
	}

	public record Dispatcher(Duration pollInterval, int batchMax, Duration claimLease) {

		public Dispatcher {
			if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
				throw new IllegalArgumentException("paymentflow.processor.dispatcher.poll-interval must be positive");
			}
			if (batchMax < 1) {
				throw new IllegalArgumentException("paymentflow.processor.dispatcher.batch-max must be positive");
			}
			if (claimLease == null || claimLease.isNegative() || claimLease.isZero()) {
				throw new IllegalArgumentException("paymentflow.processor.dispatcher.claim-lease must be positive");
			}
		}
	}
}
