package jhsoftware.paymentsflow.processor.service;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationCommandDecoder;
import jhsoftware.paymentsflow.processor.model.PaymentOperationContext;
import jhsoftware.paymentsflow.processor.observability.ReconciliationScannerHealth;
import jhsoftware.paymentsflow.processor.repository.PaymentOperationProcessingRepository;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationProcessor {

	private static final String UNKNOWN_STATUS = "UNKNOWN";
	private static final String DUPLICATE_REPLAY_METRIC = "paymentflow.duplicate.replay";

	private final PaymentOperationCommandDecoder decoder;
	private final PaymentOperationProcessingRepository repository;
	private final PaymentOperationAttemptExecutor attemptExecutor;
	private final PaymentOperationCaptureAttemptExecutor captureAttemptExecutor;
	private final PaymentOperationVoidAttemptExecutor voidAttemptExecutor;
	private final ReconciliationScannerHealth scannerHealth;
	private final Clock clock;
	private final MeterRegistry meterRegistry;

	PaymentOperationProcessor(
			PaymentOperationCommandDecoder decoder,
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor attemptExecutor,
			PaymentOperationCaptureAttemptExecutor captureAttemptExecutor,
			PaymentOperationVoidAttemptExecutor voidAttemptExecutor,
			ReconciliationScannerHealth scannerHealth,
			Clock clock,
			MeterRegistry meterRegistry) {
		this.decoder = decoder;
		this.repository = repository;
		this.attemptExecutor = attemptExecutor;
		this.captureAttemptExecutor = captureAttemptExecutor;
		this.voidAttemptExecutor = voidAttemptExecutor;
		this.scannerHealth = scannerHealth;
		this.clock = clock;
		this.meterRegistry = meterRegistry;
	}

	boolean process(String recordKey, GenericRecord recordValue) {
		PaymentOperationCommand command = decoder.decode(recordValue);
		CorrelationContext.put(CorrelationContext.CORRELATION_ID, command.correlationId());
		CorrelationContext.put(CorrelationContext.OPERATION_ID, command.operationId());
		CorrelationContext.put(CorrelationContext.MESSAGE_ID, command.messageId());

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
			String source = command.causationId() == null ? "KAFKA_REDELIVERY" : "OUTBOX_REPUBLISH";
			Counter.builder(DUPLICATE_REPLAY_METRIC).tag("source", source).register(meterRegistry).increment();
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
