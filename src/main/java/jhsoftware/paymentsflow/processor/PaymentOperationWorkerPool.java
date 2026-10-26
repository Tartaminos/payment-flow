package jhsoftware.paymentsflow.processor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PaymentOperationWorkerPool {

	private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

	private final ExecutorService executor;

	PaymentOperationWorkerPool(ProcessorProperties properties) {
		this.executor = Executors.newFixedThreadPool(properties.workers().poolSize());
	}

	void submit(Runnable task) {
		executor.execute(task);
	}

	@PreDestroy
	void shutdown() {
		executor.shutdown();
		try {
			if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			executor.shutdownNow();
		}
	}
}
