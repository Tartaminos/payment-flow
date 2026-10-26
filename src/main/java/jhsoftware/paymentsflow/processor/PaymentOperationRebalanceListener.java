package jhsoftware.paymentsflow.processor;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

class PaymentOperationRebalanceListener implements ConsumerAwareRebalanceListener {

	private final PaymentWorkAdmission admission;

	PaymentOperationRebalanceListener(PaymentWorkAdmission admission) {
		this.admission = admission;
	}

	@Override
	public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			admission.bumpEpoch(partition);
			admission.quarantinePartition(partition);
		}
	}
}
