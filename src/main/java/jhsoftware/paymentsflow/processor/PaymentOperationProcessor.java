package jhsoftware.paymentsflow.processor;

import java.util.Optional;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationProcessor {

	private static final Logger logger = LoggerFactory.getLogger(PaymentOperationProcessor.class);

	private final PaymentOperationCommandDecoder decoder;
	private final PaymentOperationProcessingRepository repository;
	private final PaymentOperationAttemptExecutor attemptExecutor;

	PaymentOperationProcessor(
			PaymentOperationCommandDecoder decoder,
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor attemptExecutor) {
		this.decoder = decoder;
		this.repository = repository;
		this.attemptExecutor = attemptExecutor;
	}

	boolean process(String recordKey, GenericRecord recordValue) {
		PaymentOperationCommand command = decoder.decode(recordValue);
		if (command.operationId() == null) {
			logger.error("Payment operation command is missing an operationId; withholding acknowledgment");
			return false;
		}

		Optional<PaymentOperationContext> contextOptional = repository.loadContext(command.operationId());
		if (contextOptional.isEmpty()) {
			logger.error(
					"No persisted payment operation found for {}; withholding acknowledgment",
					command.operationId());
			return false;
		}

		PaymentOperationContext context = contextOptional.get();
		UUID recordPaymentId = parsePaymentId(recordKey);
		if (recordPaymentId == null || !recordPaymentId.equals(context.paymentId())) {
			logger.error(
					"Kafka record key {} does not match the persisted payment {} for operation {}; withholding acknowledgment",
					recordKey,
					context.paymentId(),
					command.operationId());
			return false;
		}

		if (context.isFinalized()) {
			return true;
		}

		if (context.isUnknownAwaitingReconciliation()) {
			return true;
		}

		if (!"AUTHORIZE".equals(context.operationType())) {
			logger.error(
					"Operation {} has unsupported type {} for the authorization processor; withholding acknowledgment",
					command.operationId(),
					context.operationType());
			return false;
		}

		return attemptExecutor.execute(context, command.correlationId(), command.causationId());
	}

	private UUID parsePaymentId(String recordKey) {
		try {
			return recordKey == null ? null : UUID.fromString(recordKey);
		}
		catch (IllegalArgumentException exception) {
			return null;
		}
	}
}
