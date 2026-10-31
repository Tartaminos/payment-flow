package jhsoftware.paymentsflow.processor.service;

import java.time.Duration;
import java.util.function.DoubleSupplier;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class RetryBackoffPolicy {

	private static final long MAX_DELAY_MILLIS = 300_000L;

	private final DoubleSupplier random;

	RetryBackoffPolicy() {
		this(Math::random);
	}

	public RetryBackoffPolicy(DoubleSupplier random) {
		this.random = random;
	}

	public Duration delay(int retryNumber) {
		if (retryNumber < 1) {
			throw new IllegalArgumentException("retryNumber must be positive");
		}
		double sample = random.getAsDouble();
		if (sample < 0.0 || sample >= 1.0) {
			throw new IllegalStateException("Random sample must be in [0, 1)");
		}
		long nominalMillis = retryNumber >= 6 ? MAX_DELAY_MILLIS : 10_000L << (retryNumber - 1);
		double jitterFactor = 0.8 + (sample * 0.4);
		long jitteredMillis = Math.round(nominalMillis * jitterFactor);
		return Duration.ofMillis(Math.min(MAX_DELAY_MILLIS, jitteredMillis));
	}
}
