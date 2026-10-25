package jhsoftware.paymentsflow.processor;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ProcessorProperties.class)
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationConsumerConfiguration {

	static final String LISTENER_CONTAINER_FACTORY = "paymentOperationKafkaListenerContainerFactory";

	@Bean(LISTENER_CONTAINER_FACTORY)
	ConcurrentKafkaListenerContainerFactory<String, Object> paymentOperationKafkaListenerContainerFactory(
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(toTypedConsumerFactory(kafkaConsumerFactory));
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		return factory;
	}

	@SuppressWarnings("unchecked")
	private ConsumerFactory<String, Object> toTypedConsumerFactory(ConsumerFactory<Object, Object> consumerFactory) {
		return (ConsumerFactory<String, Object>) (ConsumerFactory<?, ?>) consumerFactory;
	}

	@Bean
	RestClient pspAuthorizationRestClient(ProcessorProperties properties) {
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		requestFactory.setConnectTimeout(properties.pspConnectTimeout());
		requestFactory.setReadTimeout(properties.pspReadTimeout());
		return RestClient.builder()
				.baseUrl(properties.pspBaseUrl().toString())
				.requestFactory(requestFactory)
				.build();
	}
}
