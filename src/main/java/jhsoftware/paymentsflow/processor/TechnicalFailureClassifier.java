package jhsoftware.paymentsflow.processor;

import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class TechnicalFailureClassifier {

	static final String DATA_ACCESS_UNAVAILABLE = "DATA_ACCESS_UNAVAILABLE";

	Optional<String> classifyTransient(Throwable failure) {
		if (failure instanceof TransientDataAccessException
				|| failure instanceof RecoverableDataAccessException
				|| failure instanceof DataAccessResourceFailureException) {
			return Optional.of(DATA_ACCESS_UNAVAILABLE);
		}
		return Optional.empty();
	}
}
