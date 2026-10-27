package jhsoftware.paymentsflow.payment;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(CaptureAuthorizationValidityProperties.class)
class PaymentConfiguration {

	@Bean
	Clock paymentClock() {
		return Clock.systemUTC();
	}

	@Bean
	RestClient captureAuthorizationValidityRestClient(CaptureAuthorizationValidityProperties properties) {
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		requestFactory.setConnectTimeout(properties.pspConnectTimeout());
		requestFactory.setReadTimeout(properties.pspReadTimeout());
		return RestClient.builder()
				.baseUrl(properties.pspBaseUrl().toString())
				.requestFactory(requestFactory)
				.build();
	}
}
