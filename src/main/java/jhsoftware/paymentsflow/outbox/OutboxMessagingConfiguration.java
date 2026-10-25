package jhsoftware.paymentsflow.outbox;

import java.io.IOException;
import java.io.InputStream;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
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
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OutboxMessagingProperties.class)
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class OutboxMessagingConfiguration {

	static final String TOPIC = "payment.operations.requested.v1";
	static final String SUBJECT = "com.paymentflow.commands.ExecutePaymentOperation";
	static final String COMPATIBILITY = "FULL_TRANSITIVE";

	@Bean
	NewTopic paymentOperationsRequestedTopic() {
		return TopicBuilder.name(TOPIC)
				.partitions(4)
				.replicas(1)
				.config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
				.build();
	}

	@Bean
	Schema executePaymentOperationSchema() {
		ClassPathResource resource = new ClassPathResource(
				"avro/com/paymentflow/commands/ExecutePaymentOperation.avsc");
		try (InputStream input = resource.getInputStream()) {
			Schema schema = new Schema.Parser().parse(input);
			if (!SUBJECT.equals(schema.getFullName())) {
				throw new IllegalStateException("Unexpected ExecutePaymentOperation Avro record name");
			}
			return schema;
		}
		catch (IOException exception) {
			throw new IllegalStateException("Cannot load ExecutePaymentOperation Avro schema", exception);
		}
	}

	@Bean(destroyMethod = "close")
	SchemaRegistryClient schemaRegistryClient(OutboxMessagingProperties properties) {
		return new CachedSchemaRegistryClient(properties.schemaRegistryUrl().toString(), 100);
	}

	@Bean
	SchemaRegistryContract executePaymentOperationSchemaRegistration(
			SchemaRegistryClient schemaRegistryClient,
			Schema executePaymentOperationSchema) {
		try {
			schemaRegistryClient.updateCompatibility(SUBJECT, COMPATIBILITY);
			int schemaId = schemaRegistryClient.register(SUBJECT, new AvroSchema(executePaymentOperationSchema));
			String configuredCompatibility = schemaRegistryClient.getCompatibility(SUBJECT);
			if (!COMPATIBILITY.equals(configuredCompatibility)) {
				throw new IllegalStateException("Schema Registry did not enforce FULL_TRANSITIVE compatibility");
			}
			return new SchemaRegistryContract(schemaId);
		}
		catch (IOException | RestClientException exception) {
			throw new IllegalStateException("Cannot establish the ExecutePaymentOperation schema contract", exception);
		}
	}

	record SchemaRegistryContract(int schemaId) {
	}
}
