package jhsoftware.paymentsflow.processor;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationProcessor {

	private static final String UNKNOWN_STATUS = "UNKNOWN";

	private final PaymentOperationCommandDecoder decoder;
	private final PaymentOperationProcessingRepository repository;
	private final PaymentOperationAttemptExecutor attemptExecutor;
	private final PaymentOperationCaptureAttemptExecutor captureAttemptExecutor;
	private final PaymentOperationVoidAttemptExecutor voidAttemptExecutor;
	private final ReconciliationScannerHealth scannerHealth;
	private final Clock clock;

	PaymentOperationProcessor(
			PaymentOperationCommandDecoder decoder,
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor attemptExecutor,
			PaymentOperationCaptureAttemptExecutor captureAttemptExecutor,
			PaymentOperationVoidAttemptExecutor voidAttemptExecutor,
			ReconciliationScannerHealth scannerHealth,
			Clock clock) {
		this.decoder = decoder;
		this.repository = repository;
		this.attemptExecutor = attemptExecutor;
		this.captureAttemptExecutor = captureAttemptExecutor;
		this.voidAttemptExecutor = voidAttemptExecutor;
		this.scannerHealth = scannerHealth;
		this.clock = clock;
	}

	boolean process(String recordKey, GenericRecord recordValue) {
		PaymentOperationCommand command = decoder.decode(recordValue);

		Optional<PaymentOperationContext> contextOptional = repository.loadContext(command.operationId());
		if (contextOptional.isEmpty()) {
			throw new PoisonCommandException(PoisonFailureCode.OPERATION_NOT_FOUND);
		}

		PaymentOperationContext context = contextOptional.get();
		UUID recordPaymentId = parsePaymentId(recordKey);
		if (recordPaymentId == null || !recordPaymentId.equals(context.paymentId())) {
			throw new PoisonCommandException(PoisonFailureCode.INVALID_RECORD_KEY);
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
			case "VOID" -> voidAttemptExecutor.execute(context, command.correlationId(), command.causationId());
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
		throw new PoisonCommandException(PoisonFailureCode.UNSUPPORTED_OPERATION_TYPE);
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
