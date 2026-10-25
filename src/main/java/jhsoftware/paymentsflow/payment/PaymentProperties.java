package jhsoftware.paymentsflow.payment;

import java.util.Objects;
import java.util.UUID;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "paymentflow")
public record PaymentProperties(UUID merchantId) {

	public PaymentProperties {
		Objects.requireNonNull(merchantId, "paymentflow.merchant-id is required");
	}
}
