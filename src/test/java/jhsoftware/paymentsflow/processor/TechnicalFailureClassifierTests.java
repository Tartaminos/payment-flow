package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.SocketTimeoutException;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

	@Test
	void schemaRegistryNetworkFailureIsTransient() {
		assertThat(classifier.classifyTransient(
				new SerializationException("registry unavailable", new SocketTimeoutException("timeout"))))
				.contains(TechnicalFailureClassifier.SCHEMA_REGISTRY_UNAVAILABLE);
	}

	@ParameterizedTest
	@ValueSource(ints = {429, 500, 503})
	void schemaRegistryRateLimitAndServerFailuresAreTransient(int status) {
		RestClientException registryFailure = new RestClientException("registry unavailable", status, 1);

		assertThat(classifier.classifyTransient(new SerializationException("decode failed", registryFailure)))
				.contains(TechnicalFailureClassifier.SCHEMA_REGISTRY_UNAVAILABLE);
	}

	@Test
	void unknownSchemaResponseIsNotMisclassifiedAsRegistryOutage() {
		RestClientException unknownSchema = new RestClientException("schema missing", 404, 1);

		assertThat(classifier.classifyTransient(new SerializationException("decode failed", unknownSchema)))
				.isEmpty();
	}
}
