package jhsoftware.paymentsflow.processor.config;

import java.net.URI;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.messaging")
public record RetryMessagingProperties(URI schemaRegistryUrl) {

	public RetryMessagingProperties {
		if (schemaRegistryUrl == null) {
			throw new IllegalArgumentException("paymentflow.messaging.schema-registry-url is required");
		}
	}
}
