package jhsoftware.paymentsflow.processor;

import java.net.URI;
import java.time.Duration;
import java.util.List;

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
	List<PspAuthorizationClient> pspAuthorizationAttemptClients(ProcessorProperties properties) {
		return properties.pspAttemptReadTimeouts().stream()
				.map(readTimeout -> new PspAuthorizationClient(
						buildRestClient(properties.pspBaseUrl(), properties.pspConnectTimeout(), readTimeout)))
				.toList();
	}

	@Bean
	RestClient pspLookupRestClient(ProcessorProperties properties) {
		return buildRestClient(properties.pspBaseUrl(), properties.pspConnectTimeout(), properties.pspLookupReadTimeout());
	}

	private RestClient buildRestClient(URI baseUrl, Duration connectTimeout, Duration readTimeout) {
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		requestFactory.setConnectTimeout(connectTimeout);
		requestFactory.setReadTimeout(readTimeout);
		return RestClient.builder()
				.baseUrl(baseUrl.toString())
				.requestFactory(requestFactory)
				.build();
	}
}
