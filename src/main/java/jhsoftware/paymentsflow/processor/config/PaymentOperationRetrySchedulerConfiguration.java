package jhsoftware.paymentsflow.processor.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationRetrySchedulerConfiguration {

	public static final String LISTENER_CONTAINER_FACTORY = "paymentOperationRetrySchedulerContainerFactory";

	@Bean(LISTENER_CONTAINER_FACTORY)
	ConcurrentKafkaListenerContainerFactory<byte[], Object> paymentOperationRetrySchedulerContainerFactory(
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<byte[], Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(toTypedConsumerFactory(kafkaConsumerFactory));
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		factory.setCommonErrorHandler(
				new DefaultErrorHandler(new FixedBackOff(2000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
		factory.setConcurrency(1);
		return factory;
	}

	@SuppressWarnings("unchecked")
	private ConsumerFactory<byte[], Object> toTypedConsumerFactory(
			ConsumerFactory<Object, Object> consumerFactory) {
		return (ConsumerFactory<byte[], Object>) (ConsumerFactory<?, ?>) consumerFactory;
	}
}
