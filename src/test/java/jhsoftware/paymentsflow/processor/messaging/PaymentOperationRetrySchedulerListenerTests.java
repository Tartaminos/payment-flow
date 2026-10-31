package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.PaymentOperationRetryMessagingConfiguration;
import jhsoftware.paymentsflow.processor.service.PaymentOperationRetryScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class PaymentOperationRetrySchedulerListenerTests {

	@Test
	void acknowledgesOnlyAfterTheSchedulerPersistsSuccessfully() {
		PaymentOperationRetryScheduler scheduler = mock(PaymentOperationRetryScheduler.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<byte[], Object> record = record();
		when(scheduler.schedule(record)).thenReturn(true);

		new PaymentOperationRetrySchedulerListener(scheduler).onRetry(record, acknowledgment);

		verify(acknowledgment).acknowledge();
	}

	@Test
	void storageFailureLeavesTheRetryTopicRecordUnacknowledged() {
		PaymentOperationRetryScheduler scheduler = mock(PaymentOperationRetryScheduler.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<byte[], Object> record = record();
		RuntimeException failure = new RuntimeException("database unavailable");
		when(scheduler.schedule(record)).thenThrow(failure);

		org.assertj.core.api.Assertions.assertThatThrownBy(
				() -> new PaymentOperationRetrySchedulerListener(scheduler).onRetry(record, acknowledgment))
				.isSameAs(failure);
		verify(acknowledgment, never()).acknowledge();
	}

	private ConsumerRecord<byte[], Object> record() {
		return new ConsumerRecord<>(PaymentOperationRetryMessagingConfiguration.TOPIC, 0, 1L,
				new byte[] {1}, new byte[] {2});
	}
}
