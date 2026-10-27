package jhsoftware.paymentsflow.processor;

import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;

record QueuedCommand(
		ConsumerRecord<byte[], Object> record,
		Acknowledgment acknowledgment,
		TopicPartition partition,
		UUID paymentId,
		long epoch) {
}
