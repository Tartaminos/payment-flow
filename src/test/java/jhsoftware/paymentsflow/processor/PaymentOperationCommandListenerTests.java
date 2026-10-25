package jhsoftware.paymentsflow.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class PaymentOperationCommandListenerTests {

	@Test
	void acknowledgesWhenProcessorReportsAckEligible() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord();
		when(processor.process(record.key(), (GenericRecord) record.value())).thenReturn(true);

		new PaymentOperationCommandListener(processor).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment).acknowledge();
	}

	@Test
	void withholdsAcknowledgmentWhenProcessorReportsUnresolvedOutcome() {
		PaymentOperationProcessor processor = mock(PaymentOperationProcessor.class);
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		ConsumerRecord<String, Object> record = consumerRecord();
		when(processor.process(record.key(), (GenericRecord) record.value())).thenReturn(false);

		new PaymentOperationCommandListener(processor).onPaymentOperationCommand(record, acknowledgment);

		verify(acknowledgment, never()).acknowledge();
	}

	private ConsumerRecord<String, Object> consumerRecord() {
		GenericRecord value = mock(GenericRecord.class);
		return new ConsumerRecord<>("payment.operations.requested.v1", 0, 0L, "key", value);
	}
}
