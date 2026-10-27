package jhsoftware.paymentsflow.processor;

import java.io.IOException;
import java.io.InputStream;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RetryMessagingProperties.class)
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationRetryMessagingConfiguration {

	static final String TOPIC = "payment.operations.retry.v1";
	static final String SUBJECT = "com.paymentflow.retry.PaymentOperationRetry";
	static final String COMPATIBILITY = "FULL_TRANSITIVE";

	@Bean
	NewTopic paymentOperationsRetryTopic() {
		return TopicBuilder.name(TOPIC)
				.partitions(2)
				.replicas(1)
				.config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
				.build();
	}

	@Bean
	Schema paymentOperationRetrySchema() {
		ClassPathResource resource = new ClassPathResource(
				"avro/com/paymentflow/retry/PaymentOperationRetry.avsc");
		try (InputStream input = resource.getInputStream()) {
			Schema schema = new Schema.Parser().parse(input);
			if (!SUBJECT.equals(schema.getFullName())) {
				throw new IllegalStateException("Unexpected PaymentOperationRetry Avro record name");
			}
			return schema;
		}
		catch (IOException exception) {
			throw new IllegalStateException("Cannot load PaymentOperationRetry Avro schema", exception);
		}
	}

	@Bean
	SchemaRegistryContract paymentOperationRetrySchemaRegistration(
			SchemaRegistryClient schemaRegistryClient,
			Schema paymentOperationRetrySchema) {
		try {
			schemaRegistryClient.updateCompatibility(SUBJECT, COMPATIBILITY);
			int schemaId = schemaRegistryClient.register(SUBJECT, new AvroSchema(paymentOperationRetrySchema));
			String configuredCompatibility = schemaRegistryClient.getCompatibility(SUBJECT);
			if (!COMPATIBILITY.equals(configuredCompatibility)) {
				throw new IllegalStateException("Schema Registry did not enforce FULL_TRANSITIVE compatibility");
			}
			return new SchemaRegistryContract(schemaId);
		}
		catch (IOException | RestClientException exception) {
			throw new IllegalStateException("Cannot establish the PaymentOperationRetry schema contract", exception);
		}
	}

	record SchemaRegistryContract(int schemaId) {
	}
}
