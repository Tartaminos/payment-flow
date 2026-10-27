package jhsoftware.paymentsflow.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import jhsoftware.paymentsflow.observability.CorrelationContext;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationRetrySchedulerListener {

	static final String LISTENER_ID = "payment-operation-retry-scheduler-listener";
	static final String GROUP_ID = "paymentflow-retry-scheduler-v1";

	private final PaymentOperationRetryScheduler scheduler;

	PaymentOperationRetrySchedulerListener(PaymentOperationRetryScheduler scheduler) {
		this.scheduler = scheduler;
	}

	@KafkaListener(
			id = LISTENER_ID,
			groupId = GROUP_ID,
			topics = PaymentOperationRetryMessagingConfiguration.TOPIC,
			concurrency = "1",
			containerFactory = PaymentOperationRetrySchedulerConfiguration.LISTENER_CONTAINER_FACTORY)
	void onRetry(ConsumerRecord<byte[], Object> record, Acknowledgment acknowledgment) {
		try {
			if (scheduler.schedule(record)) {
				acknowledgment.acknowledge();
			}
		}
		finally {
			CorrelationContext.clear();
		}
	}
}
