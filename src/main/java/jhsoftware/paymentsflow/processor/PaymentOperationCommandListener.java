package jhsoftware.paymentsflow.processor;

import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.outbox.OutboxMessagingConfiguration;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationCommandListener {

	static final String LISTENER_ID = "payment-operation-command-listener";

	private final PaymentOperationProcessor processor;
	private final PaymentWorkAdmission admission;
	private final PaymentOperationWorkerPool workerPool;

	PaymentOperationCommandListener(
			PaymentOperationProcessor processor,
			PaymentWorkAdmission admission,
			PaymentOperationWorkerPool workerPool) {
		this.processor = processor;
		this.admission = admission;
		this.workerPool = workerPool;
	}

	@KafkaListener(
			id = LISTENER_ID,
			topics = OutboxMessagingConfiguration.TOPIC,
			containerFactory = PaymentOperationConsumerConfiguration.LISTENER_CONTAINER_FACTORY)
	void onPaymentOperationCommand(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
		resumeReadyPartitions();

		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		UUID paymentId = parsePaymentId(record.key());
		long epoch = admission.currentEpoch(partition);
		QueuedCommand command = new QueuedCommand(record, acknowledgment, partition, paymentId, epoch);

		PaymentWorkAdmission.AdmissionResult result = admission.tryAdmit(paymentId, partition, command);
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
			boolean ackEligible = processor.process(
					command.record().key(), (GenericRecord) command.record().value());
			if (ackEligible && admission.currentEpoch(command.partition()) == command.epoch()) {
				command.acknowledgment().acknowledge();
			}
		}
		finally {
			admission.release(command.paymentId(), command.partition()).ifPresent(this::dispatch);
			resumeReadyPartitions();
		}
	}

	private void resumeReadyPartitions() {
		admission.partitionsNeedingResume();
	}

	private UUID parsePaymentId(String recordKey) {
		try {
			return recordKey == null ? UUID.randomUUID() : UUID.fromString(recordKey);
		}
		catch (IllegalArgumentException exception) {
			return UUID.randomUUID();
		}
	}
}
