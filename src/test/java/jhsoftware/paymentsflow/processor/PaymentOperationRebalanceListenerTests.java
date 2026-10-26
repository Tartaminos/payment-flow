package jhsoftware.paymentsflow.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class PaymentOperationRebalanceListenerTests {

	@Test
	void revokedPartitionsBumpEpochAndAreQuarantined() {
		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		TopicPartition partition = new TopicPartition("payment.operations.requested.v1", 0);

		new PaymentOperationRebalanceListener(admission).onPartitionsRevokedBeforeCommit(consumer, List.of(partition));

		verify(admission).bumpEpoch(partition);
		verify(admission).quarantinePartition(partition);
	}
}
