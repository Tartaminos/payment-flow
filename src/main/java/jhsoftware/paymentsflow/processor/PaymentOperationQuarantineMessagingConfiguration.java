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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationQuarantineMessagingConfiguration {

	static final String TOPIC = "payment.operations.dlq.v1";
	static final String SUBJECT = "com.paymentflow.dlq.PaymentOperationQuarantine";
	static final String COMPATIBILITY = "FULL_TRANSITIVE";

	@Bean
	NewTopic paymentOperationsDlqTopic() {
		return TopicBuilder.name(TOPIC)
				.partitions(1)
				.replicas(1)
				.config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
				.build();
	}

	@Bean
	Schema paymentOperationQuarantineSchema() {
		ClassPathResource resource = new ClassPathResource(
				"avro/com/paymentflow/dlq/PaymentOperationQuarantine.avsc");
		try (InputStream input = resource.getInputStream()) {
			Schema schema = new Schema.Parser().parse(input);
			if (!SUBJECT.equals(schema.getFullName())) {
				throw new IllegalStateException("Unexpected PaymentOperationQuarantine Avro record name");
			}
			return schema;
		}
		catch (IOException exception) {
			throw new IllegalStateException("Cannot load PaymentOperationQuarantine Avro schema", exception);
		}
	}

	@Bean
	SchemaRegistryContract paymentOperationQuarantineSchemaRegistration(
			SchemaRegistryClient schemaRegistryClient,
			@Qualifier("paymentOperationQuarantineSchema") Schema paymentOperationQuarantineSchema) {
		try {
			schemaRegistryClient.updateCompatibility(SUBJECT, COMPATIBILITY);
			int schemaId = schemaRegistryClient.register(SUBJECT, new AvroSchema(paymentOperationQuarantineSchema));
			String configuredCompatibility = schemaRegistryClient.getCompatibility(SUBJECT);
			if (!COMPATIBILITY.equals(configuredCompatibility)) {
				throw new IllegalStateException("Schema Registry did not enforce FULL_TRANSITIVE compatibility");
			}
			return new SchemaRegistryContract(schemaId);
		}
		catch (IOException | RestClientException exception) {
			throw new IllegalStateException("Cannot establish the PaymentOperationQuarantine schema contract", exception);
		}
	}

	record SchemaRegistryContract(int schemaId) {
	}
}
