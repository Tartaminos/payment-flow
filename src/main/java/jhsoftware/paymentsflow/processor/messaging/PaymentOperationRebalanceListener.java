package jhsoftware.paymentsflow.processor.messaging;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

public class PaymentOperationRebalanceListener implements ConsumerAwareRebalanceListener {

	private final PaymentWorkAdmission admission;

	public PaymentOperationRebalanceListener(PaymentWorkAdmission admission) {
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
