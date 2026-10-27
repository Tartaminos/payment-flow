package jhsoftware.paymentsflow.processor;

import java.net.URI;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.messaging")
record RetryMessagingProperties(URI schemaRegistryUrl) {

	RetryMessagingProperties {
		if (schemaRegistryUrl == null) {
			throw new IllegalArgumentException("paymentflow.messaging.schema-registry-url is required");
		}
	}
}
