package jhsoftware.paymentsflow.processor.observability;

import jhsoftware.paymentsflow.processor.config.ProcessorProperties;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class ReconciliationScannerHealth {

	private static final int STALENESS_MULTIPLIER = 3;

	private final Duration stalenessThreshold;
	private final AtomicReference<Instant> lastHeartbeatAt = new AtomicReference<>();

	ReconciliationScannerHealth(ProcessorProperties properties) {
		this.stalenessThreshold = properties.reconciliation().pollInterval().multipliedBy(STALENESS_MULTIPLIER);
	}

	public void recordHeartbeat(Instant now) {
		lastHeartbeatAt.set(now);
	}

	public boolean isOperational(Instant now) {
		Instant lastHeartbeat = lastHeartbeatAt.get();
		if (lastHeartbeat == null) {
			return false;
		}
		return Duration.between(lastHeartbeat, now).compareTo(stalenessThreshold) <= 0;
	}
}
