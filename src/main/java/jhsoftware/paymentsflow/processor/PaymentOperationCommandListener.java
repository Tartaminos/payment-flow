package jhsoftware.paymentsflow.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.outbox.OutboxMessagingConfiguration;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationCommandListener {

	private final PaymentOperationProcessor processor;

	PaymentOperationCommandListener(PaymentOperationProcessor processor) {
		this.processor = processor;
	}

	@KafkaListener(
			topics = OutboxMessagingConfiguration.TOPIC,
			containerFactory = PaymentOperationConsumerConfiguration.LISTENER_CONTAINER_FACTORY)
	void onPaymentOperationCommand(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
		boolean ackEligible = processor.process(record.key(), (GenericRecord) record.value());
		if (ackEligible) {
			acknowledgment.acknowledge();
		}
	}
}
