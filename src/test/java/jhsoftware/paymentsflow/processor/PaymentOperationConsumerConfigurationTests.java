package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

class PaymentOperationConsumerConfigurationTests {

	@Test
	void listenerContainerFactoryNeverAutoCommitsOffsetsOnPoll() {
		PaymentOperationConsumerConfiguration configuration = new PaymentOperationConsumerConfiguration();
		@SuppressWarnings("unchecked")
		ConsumerFactory<Object, Object> consumerFactory = mock(ConsumerFactory.class);
		PaymentWorkAdmission admission = mock(PaymentWorkAdmission.class);

		ConcurrentKafkaListenerContainerFactory<String, Object> factory =
				configuration.paymentOperationKafkaListenerContainerFactory(consumerFactory, admission);

		assertThat(factory.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
	}
}
