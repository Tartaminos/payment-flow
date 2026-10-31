package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.PaymentOperationQuarantineMessagingConfiguration;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationQuarantineEnvelopeDto;

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

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationQuarantinePublisher {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationQuarantinePublisher.class);
	private static final Duration DELIVERY_DEADLINE = Duration.ofSeconds(30);

	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final Schema schema;

	public PaymentOperationQuarantinePublisher(
			KafkaTemplate<String, Object> kafkaTemplate,
			@Qualifier("paymentOperationQuarantineSchema") Schema schema,
			PaymentOperationQuarantineMessagingConfiguration.SchemaRegistryContract schemaRegistryContract) {
		this.kafkaTemplate = kafkaTemplate;
		this.schema = schema;
	}

	public boolean publish(PaymentOperationQuarantineEnvelopeDto envelope) {
		try {
			kafkaTemplate.send(
					PaymentOperationQuarantineMessagingConfiguration.TOPIC,
					envelope.quarantineId().toString(),
					toRecord(envelope))
					.get(DELIVERY_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
			return true;
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			logger.warn("Quarantine publication interrupted for quarantine {}", envelope.quarantineId());
			return false;
		}
		catch (Exception exception) {
			logger.warn("Quarantine publication failed for quarantine {} ({})",
					envelope.quarantineId(), rootCause(exception).getClass().getSimpleName());
			return false;
		}
	}

	private GenericRecord toRecord(PaymentOperationQuarantineEnvelopeDto envelope) {
		GenericRecord record = new GenericData.Record(schema);
		record.put("quarantineId", envelope.quarantineId().toString());
		record.put("sourceTopic", envelope.sourceTopic());
		record.put("sourcePartition", envelope.sourcePartition());
		record.put("sourceOffset", envelope.sourceOffset());
		record.put("sourceRecordKey", ByteBuffer.wrap(envelope.sourceRecordKey()));
		record.put("sourceRecordValue", ByteBuffer.wrap(envelope.sourceRecordValue()));
		record.put("failureCode", envelope.failureCode());
		record.put("quarantinedAt", envelope.quarantinedAt().toEpochMilli());
		record.put("operationId", envelope.operationId() == null ? null : envelope.operationId().toString());
		record.put("correlationId", envelope.correlationId() == null ? null : envelope.correlationId().toString());
		record.put("sourceMessageId", envelope.sourceMessageId() == null ? null : envelope.sourceMessageId().toString());
		return record;
	}

	private Throwable rootCause(Throwable failure) {
		Throwable cause = failure;
		while (cause.getCause() != null && cause.getCause() != cause) {
			cause = cause.getCause();
		}
		return cause;
	}
}
