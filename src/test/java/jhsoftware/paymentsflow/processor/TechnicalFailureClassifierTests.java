package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;

class TechnicalFailureClassifierTests {

	private final TechnicalFailureClassifier classifier = new TechnicalFailureClassifier();

	@Test
	void dataAccessResourceFailureIsClassifiedAsTransient() {
		assertThat(classifier.classifyTransient(new DataAccessResourceFailureException("db down")))
				.isPresent();
	}

	@Test
	void unrelatedRuntimeExceptionIsNotClassified() {
		assertThat(classifier.classifyTransient(new IllegalStateException("not transient")))
				.isEmpty();
	}

	@Test
	void permanentDataIntegrityFailureIsNotClassifiedAsTransient() {
		assertThat(classifier.classifyTransient(new DataIntegrityViolationException("invalid persisted data")))
				.isEmpty();
	}
}
