package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.PaymentOperationConsumerConfiguration;
import jhsoftware.paymentsflow.processor.observability.BlockedPartitionDiagnostics;
import jhsoftware.paymentsflow.processor.service.PaymentOperationTechnicalRetryHandoff;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;
import jhsoftware.paymentsflow.outbox.OutboxMessagingConfiguration;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationCommandListener {

	public static final String LISTENER_ID = "payment-operation-command-listener";
	private static final String COMMANDS_CONSUMED_METRIC = "paymentflow.commands.consumed";

	private final PaymentOperationTechnicalRetryHandoff retryHandoff;
	private final PaymentWorkAdmission admission;
	private final PaymentOperationWorkerPool workerPool;
	private final BlockedPartitionDiagnostics blockedPartitionDiagnostics;
	private final MeterRegistry meterRegistry;

	PaymentOperationCommandListener(
			PaymentOperationTechnicalRetryHandoff retryHandoff,
			PaymentWorkAdmission admission,
			PaymentOperationWorkerPool workerPool,
			BlockedPartitionDiagnostics blockedPartitionDiagnostics,
			MeterRegistry meterRegistry) {
		this.retryHandoff = retryHandoff;
		this.admission = admission;
		this.workerPool = workerPool;
		this.blockedPartitionDiagnostics = blockedPartitionDiagnostics;
		this.meterRegistry = meterRegistry;
	}

	@KafkaListener(
			id = LISTENER_ID,
			topics = OutboxMessagingConfiguration.TOPIC,
			containerFactory = PaymentOperationConsumerConfiguration.LISTENER_CONTAINER_FACTORY)
	void onPaymentOperationCommand(ConsumerRecord<byte[], Object> record, Acknowledgment acknowledgment) {
		resumeReadyPartitions();

		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		UUID paymentId = parsePaymentId(record.key());
		long epoch = admission.currentEpoch(partition);
		QueuedCommand command = new QueuedCommand(record, acknowledgment, partition, paymentId, epoch);

		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command);
		Counter.builder(COMMANDS_CONSUMED_METRIC).tag("outcome", result.name()).register(meterRegistry).increment();
		switch (result) {
			case ADMIT -> dispatch(command);
			case QUEUED -> { }
			case OVERFLOW -> { }
		}
	}

	private void dispatch(QueuedCommand command) {
		workerPool.submit(() -> runAndRelease(command));
	}

	private void runAndRelease(QueuedCommand command) {
		try {
			CorrelationContext.put(CorrelationContext.PAYMENT_ID, command.paymentId());
			boolean ackEligible = retryHandoff.handle(command.record());
			if (ackEligible) {
				blockedPartitionDiagnostics.recordEligible(command.partition());
			}
			else {
				blockedPartitionDiagnostics.recordIneligible(command.partition(), Instant.now());
			}
			if (ackEligible && admission.currentEpoch(command.partition()) == command.epoch()) {
				command.acknowledgment().acknowledge();
			}
		}
		finally {
			admission.release(command.paymentId(), command.partition()).ifPresent(this::dispatch);
			resumeReadyPartitions();
			CorrelationContext.clear();
		}
	}

	private void resumeReadyPartitions() {
		for (TopicPartition partition : admission.partitionsNeedingResume()) {
			for (QueuedCommand command : admission.drainOverflow(partition)) {
				dispatch(command);
			}
		}
	}

	private UUID parsePaymentId(byte[] recordKey) {
		try {
			if (recordKey == null) {
				return UUID.randomUUID();
			}
			String key = StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(recordKey))
					.toString();
			return UUID.fromString(key);
		}
		catch (IllegalArgumentException | CharacterCodingException exception) {
			return UUID.randomUUID();
		}
	}
}
