package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.config.ProcessorProperties;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.slf4j.MDC;
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
		Map<String, String> callerContext = MDC.getCopyOfContextMap();
		executor.execute(() -> runWithPropagatedContext(task, callerContext));
	}

	private void runWithPropagatedContext(Runnable task, Map<String, String> callerContext) {
		try {
			if (callerContext != null) {
				MDC.setContextMap(callerContext);
			}
			task.run();
		}
		finally {
			MDC.clear();
		}
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
