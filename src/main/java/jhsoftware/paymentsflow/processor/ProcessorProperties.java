package jhsoftware.paymentsflow.processor;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.processor")
record ProcessorProperties(
		URI pspBaseUrl,
		Duration pspConnectTimeout,
		List<Duration> pspAttemptReadTimeouts,
		Duration pspLookupReadTimeout,
		Duration operationLeaseDuration,
		Duration retryGapAfterFirstAttempt,
		Duration retryGapAfterSecondAttempt,
		Reconciliation reconciliation) {

	ProcessorProperties {
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
	}

	record Reconciliation(Duration pollInterval, int batchSize, Duration claimLease, Duration backoffInitial, Duration backoffMax) {

		Reconciliation {
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
}
