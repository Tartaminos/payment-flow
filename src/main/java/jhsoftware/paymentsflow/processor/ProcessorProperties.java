package jhsoftware.paymentsflow.processor;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.processor")
record ProcessorProperties(URI pspBaseUrl, Duration pspConnectTimeout, Duration pspReadTimeout) {

	ProcessorProperties {
		if (pspBaseUrl == null) {
			throw new IllegalArgumentException("paymentflow.processor.psp-base-url is required");
		}
		if (pspConnectTimeout == null || pspConnectTimeout.isNegative() || pspConnectTimeout.isZero()) {
			throw new IllegalArgumentException("paymentflow.processor.psp-connect-timeout must be positive");
		}
		if (pspReadTimeout == null || pspReadTimeout.isNegative() || pspReadTimeout.isZero()) {
			throw new IllegalArgumentException("paymentflow.processor.psp-read-timeout must be positive");
		}
	}
}
