package jhsoftware.paymentsflow.processor;

import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationRawValueDecoder {

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
		return (GenericRecord) deserializer.deserialize(topic, bytes);
	}
}
