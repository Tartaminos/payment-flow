package jhsoftware.paymentsflow.payment;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class PaymentConfiguration {

	@Bean
	Clock paymentClock() {
		return Clock.systemUTC();
	}
}
