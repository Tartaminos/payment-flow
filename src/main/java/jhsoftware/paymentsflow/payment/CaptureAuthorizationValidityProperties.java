package jhsoftware.paymentsflow.payment;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "paymentflow.capture-validity")
record CaptureAuthorizationValidityProperties(URI pspBaseUrl, Duration pspConnectTimeout, Duration pspReadTimeout) {

	CaptureAuthorizationValidityProperties {
		if (pspBaseUrl == null) {
			throw new IllegalArgumentException("paymentflow.capture-validity.psp-base-url is required");
		}
		if (pspConnectTimeout == null || pspConnectTimeout.isNegative() || pspConnectTimeout.isZero()) {
			throw new IllegalArgumentException("paymentflow.capture-validity.psp-connect-timeout must be positive");
		}
		if (pspReadTimeout == null || pspReadTimeout.isNegative() || pspReadTimeout.isZero()) {
			throw new IllegalArgumentException("paymentflow.capture-validity.psp-read-timeout must be positive");
		}
	}
}
