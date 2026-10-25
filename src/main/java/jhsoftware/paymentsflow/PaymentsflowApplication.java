package jhsoftware.paymentsflow;

import jhsoftware.paymentsflow.payment.PaymentProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(PaymentProperties.class)
public class PaymentsflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentsflowApplication.class, args);
	}

}
