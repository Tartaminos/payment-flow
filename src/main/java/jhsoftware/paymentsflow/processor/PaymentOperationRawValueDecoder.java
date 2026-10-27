package jhsoftware.paymentsflow.processor;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationRawValueDecoder {

	private static final String COMMAND_RECORD = "com.paymentflow.commands.ExecutePaymentOperation";

	private final KafkaAvroDeserializer deserializer;

	PaymentOperationRawValueDecoder(RetryMessagingProperties properties) {
		this.deserializer = newDeserializer(properties);
	}

	private KafkaAvroDeserializer newDeserializer(RetryMessagingProperties properties) {
		KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
		kafkaAvroDeserializer.configure(
				Map.of(
						"schema.registry.url", properties.schemaRegistryUrl().toString(),
						"specific.avro.reader", false),
				false);
		return kafkaAvroDeserializer;
	}

	synchronized GenericRecord decode(String topic, byte[] bytes) {
		if (bytes == null || bytes.length < 5 || bytes[0] != 0) {
			throw new PoisonCommandException(PoisonFailureCode.CORRUPTED_WIRE_FORMAT);
		}
		try {
			Object value = deserializer.deserialize(topic, bytes);
			if (!(value instanceof GenericRecord record)) {
				throw new PoisonCommandException(PoisonFailureCode.INVALID_AVRO_PAYLOAD);
			}
			if (!COMMAND_RECORD.equals(record.getSchema().getFullName())) {
				throw new PoisonCommandException(PoisonFailureCode.UNSUPPORTED_SCHEMA);
			}
			return record;
		}
		catch (PoisonCommandException exception) {
			throw exception;
		}
		catch (SerializationException exception) {
			RestClientException registryFailure = causeOf(exception, RestClientException.class);
			if (registryFailure != null && registryFailure.getStatus() != 404) {
				throw exception;
			}
			if (hasNetworkFailure(exception)) {
				throw exception;
			}
			PoisonFailureCode code = registryFailure == null
					? PoisonFailureCode.INVALID_AVRO_PAYLOAD
					: PoisonFailureCode.UNSUPPORTED_SCHEMA;
			throw new PoisonCommandException(code, exception);
		}
	}

	private boolean hasNetworkFailure(Throwable failure) {
		return causeOf(failure, ConnectException.class) != null
				|| causeOf(failure, SocketTimeoutException.class) != null
				|| causeOf(failure, UnknownHostException.class) != null;
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
