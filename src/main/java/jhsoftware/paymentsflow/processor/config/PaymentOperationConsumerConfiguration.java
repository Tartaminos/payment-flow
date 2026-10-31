package jhsoftware.paymentsflow.processor.config;

import jhsoftware.paymentsflow.processor.client.PspAuthorizationClient;
import jhsoftware.paymentsflow.processor.client.PspCaptureClient;
import jhsoftware.paymentsflow.processor.client.PspVoidClient;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRebalanceListener;
import jhsoftware.paymentsflow.processor.messaging.PaymentWorkAdmission;

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
public class PaymentOperationConsumerConfiguration {

	public static final String LISTENER_CONTAINER_FACTORY = "paymentOperationKafkaListenerContainerFactory";

	@Bean(LISTENER_CONTAINER_FACTORY)
	ConcurrentKafkaListenerContainerFactory<String, Object> paymentOperationKafkaListenerContainerFactory(
			ConsumerFactory<Object, Object> kafkaConsumerFactory, PaymentWorkAdmission admission) {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(toTypedConsumerFactory(kafkaConsumerFactory));
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		factory.getContainerProperties().setAsyncAcks(true);
		factory.getContainerProperties().setConsumerRebalanceListener(new PaymentOperationRebalanceListener(admission));
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
	List<PspCaptureClient> pspCaptureAttemptClients(ProcessorProperties properties) {
		return properties.pspAttemptReadTimeouts().stream()
				.map(readTimeout -> new PspCaptureClient(
						buildRestClient(properties.pspBaseUrl(), properties.pspConnectTimeout(), readTimeout)))
				.toList();
	}

	@Bean
	List<PspVoidClient> pspVoidAttemptClients(ProcessorProperties properties) {
		return properties.pspAttemptReadTimeouts().stream()
				.map(readTimeout -> new PspVoidClient(
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
