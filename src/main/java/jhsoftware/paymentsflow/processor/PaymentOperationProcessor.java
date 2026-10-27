package jhsoftware.paymentsflow.processor;

import java.time.Clock;
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
	private static final String UNKNOWN_STATUS = "UNKNOWN";

	private final PaymentOperationCommandDecoder decoder;
	private final PaymentOperationProcessingRepository repository;
	private final PaymentOperationAttemptExecutor attemptExecutor;
	private final PaymentOperationCaptureAttemptExecutor captureAttemptExecutor;
	private final ReconciliationScannerHealth scannerHealth;
	private final Clock clock;

	PaymentOperationProcessor(
			PaymentOperationCommandDecoder decoder,
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor attemptExecutor,
			PaymentOperationCaptureAttemptExecutor captureAttemptExecutor,
			ReconciliationScannerHealth scannerHealth,
			Clock clock) {
		this.decoder = decoder;
		this.repository = repository;
		this.attemptExecutor = attemptExecutor;
		this.captureAttemptExecutor = captureAttemptExecutor;
		this.scannerHealth = scannerHealth;
		this.clock = clock;
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
			return scannerHealth.isOperational(clock.instant());
		}

		boolean ackEligible = switch (context.operationType()) {
			case "AUTHORIZE" -> attemptExecutor.execute(context, command.correlationId(), command.causationId());
			case "CAPTURE" -> captureAttemptExecutor.execute(context, command.correlationId(), command.causationId());
			default -> rejectUnsupportedType(command.operationId(), context.operationType());
		};
		if (!ackEligible) {
			return false;
		}

		if (isNowUnknown(command.operationId())) {
			return scannerHealth.isOperational(clock.instant());
		}
		return true;
	}

	private boolean rejectUnsupportedType(UUID operationId, String operationType) {
		logger.error(
				"Operation {} has unsupported type {} for the payment operation processor; withholding acknowledgment",
				operationId,
				operationType);
		return false;
	}

	private boolean isNowUnknown(UUID operationId) {
		return repository.loadContext(operationId)
				.map(refreshed -> UNKNOWN_STATUS.equals(refreshed.operationStatus()))
				.orElse(false);
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
