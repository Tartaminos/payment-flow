package jhsoftware.paymentsflow.outbox;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.messaging")
record OutboxMessagingProperties(URI schemaRegistryUrl, Publisher publisher) {

	OutboxMessagingProperties {
		if (schemaRegistryUrl == null) {
			throw new IllegalArgumentException("paymentflow.messaging.schema-registry-url is required");
		}
		if (publisher == null) {
			throw new IllegalArgumentException("paymentflow.messaging.publisher is required");
		}
	}

	record Publisher(int batchMax, Duration deliveryDeadline) {

		Publisher {
			if (batchMax < 1) {
				throw new IllegalArgumentException("paymentflow.messaging.publisher.batch-max must be positive");
			}
			if (deliveryDeadline == null || deliveryDeadline.isNegative() || deliveryDeadline.isZero()) {
				throw new IllegalArgumentException(
						"paymentflow.messaging.publisher.delivery-deadline must be positive");
			}
		}
	}
}
