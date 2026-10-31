package jhsoftware.paymentsflow.processor.service;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class TechnicalFailureClassifier {

	public static final String DATA_ACCESS_UNAVAILABLE = "DATA_ACCESS_UNAVAILABLE";
	static final String SCHEMA_REGISTRY_UNAVAILABLE = "SCHEMA_REGISTRY_UNAVAILABLE";

	Optional<String> classifyTransient(Throwable failure) {
		if (failure instanceof TransientDataAccessException
				|| failure instanceof RecoverableDataAccessException
				|| failure instanceof DataAccessResourceFailureException) {
			return Optional.of(DATA_ACCESS_UNAVAILABLE);
		}
		RestClientException registryFailure = causeOf(failure, RestClientException.class);
		if (registryFailure != null && (registryFailure.getStatus() == 429 || registryFailure.getStatus() >= 500)) {
			return Optional.of(SCHEMA_REGISTRY_UNAVAILABLE);
		}
		if (causeOf(failure, ConnectException.class) != null
				|| causeOf(failure, SocketTimeoutException.class) != null
				|| causeOf(failure, UnknownHostException.class) != null) {
			return Optional.of(SCHEMA_REGISTRY_UNAVAILABLE);
		}
		return Optional.empty();
	}

	private <T extends Throwable> T causeOf(Throwable failure, Class<T> type) {
		Throwable current = failure;
		while (current != null) {
			if (type.isInstance(current)) {
				return type.cast(current);
			}
			current = current.getCause();
		}
		return null;
	}
}
