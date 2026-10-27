package jhsoftware.paymentsflow.processor;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationRetryPublisher {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationRetryPublisher.class);
	private static final Duration DELIVERY_DEADLINE = Duration.ofSeconds(30);

	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final Schema schema;

	PaymentOperationRetryPublisher(
			KafkaTemplate<String, Object> kafkaTemplate,
			@Qualifier("paymentOperationRetrySchema") Schema paymentOperationRetrySchema,
			PaymentOperationRetryMessagingConfiguration.SchemaRegistryContract schemaRegistryContract) {
		this.kafkaTemplate = kafkaTemplate;
		this.schema = paymentOperationRetrySchema;
	}

	boolean publish(PaymentOperationRetryEnvelope envelope) {
		CorrelationContext.put(CorrelationContext.RETRY_ID, envelope.retryId());
		CorrelationContext.put(CorrelationContext.CORRELATION_ID, envelope.correlationId());
		CorrelationContext.put(CorrelationContext.OPERATION_ID, envelope.operationId());
		try {
			kafkaTemplate.send(
					PaymentOperationRetryMessagingConfiguration.TOPIC,
					envelope.retryId().toString(),
					toRecord(envelope))
					.get(DELIVERY_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
			return true;
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			logger.warn("Technical retry publication interrupted for retry {}", envelope.retryId());
			return false;
		}
		catch (Exception exception) {
			logger.warn(
					"Technical retry publication failed for retry {} ({})",
					envelope.retryId(),
					rootCause(exception).getClass().getSimpleName());
			return false;
		}
	}

	private Throwable rootCause(Throwable failure) {
		Throwable cause = failure;
		while (cause.getCause() != null && cause.getCause() != cause) {
			cause = cause.getCause();
		}
		return cause;
	}

	private GenericRecord toRecord(PaymentOperationRetryEnvelope envelope) {
		GenericRecord record = new GenericData.Record(schema);
		record.put("retryId", envelope.retryId().toString());
		record.put("sourceTopic", envelope.sourceTopic());
		record.put("sourcePartition", envelope.sourcePartition());
		record.put("sourceOffset", envelope.sourceOffset());
		record.put("sourceRecordKey", ByteBuffer.wrap(envelope.sourceRecordKey()));
		record.put("sourceRecordValue", ByteBuffer.wrap(envelope.sourceRecordValue()));
		record.put("failureClass", envelope.failureClass());
		record.put("failedAt", envelope.failedAt().toEpochMilli());
		record.put("operationId", envelope.operationId() == null ? null : envelope.operationId().toString());
		record.put("correlationId", envelope.correlationId() == null ? null : envelope.correlationId().toString());
		record.put(
				"sourceMessageId",
				envelope.sourceMessageId() == null ? null : envelope.sourceMessageId().toString());
		return record;
	}
}
