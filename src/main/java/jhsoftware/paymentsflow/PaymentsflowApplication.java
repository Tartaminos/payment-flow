package jhsoftware.paymentsflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import jhsoftware.paymentsflow.payment.config.PaymentProperties;

@SpringBootApplication
@EnableConfigurationProperties(PaymentProperties.class)
public class PaymentsflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentsflowApplication.class, args);
	}

}
