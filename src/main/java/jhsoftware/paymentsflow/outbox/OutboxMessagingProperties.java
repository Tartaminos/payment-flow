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

	record Publisher(
			int batchMax,
			Duration deliveryDeadline,
			Duration claimLease,
			Duration retryBackoffInitial,
			Duration retryBackoffMax) {

		Publisher {
			if (batchMax < 1) {
				throw new IllegalArgumentException("paymentflow.messaging.publisher.batch-max must be positive");
			}
			if (deliveryDeadline == null || deliveryDeadline.isNegative() || deliveryDeadline.isZero()) {
				throw new IllegalArgumentException(
						"paymentflow.messaging.publisher.delivery-deadline must be positive");
			}
			if (claimLease == null || claimLease.isNegative() || claimLease.isZero()) {
				throw new IllegalArgumentException("paymentflow.messaging.publisher.claim-lease must be positive");
			}
			if (claimLease.compareTo(deliveryDeadline) <= 0) {
				throw new IllegalArgumentException(
						"paymentflow.messaging.publisher.claim-lease must exceed the delivery deadline");
			}
			if (retryBackoffInitial == null || retryBackoffInitial.isNegative() || retryBackoffInitial.isZero()) {
				throw new IllegalArgumentException(
						"paymentflow.messaging.publisher.retry-backoff-initial must be positive");
			}
			if (retryBackoffMax == null || retryBackoffMax.compareTo(retryBackoffInitial) < 0) {
				throw new IllegalArgumentException(
						"paymentflow.messaging.publisher.retry-backoff-max must be at least the initial backoff");
			}
		}
	}
}
