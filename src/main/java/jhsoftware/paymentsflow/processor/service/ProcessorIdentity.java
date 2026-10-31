package jhsoftware.paymentsflow.processor.service;

import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class ProcessorIdentity {

	private final UUID id = UUID.randomUUID();

	UUID id() {
		return id;
	}
}
