package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class PaymentOperationWorkerPoolTests {

	@Test
	void inFlightTaskCompletesDuringControlledShutdown() throws InterruptedException {
		PaymentOperationWorkerPool pool = new PaymentOperationWorkerPool(properties(1));
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch finished = new CountDownLatch(1);

		pool.submit(() -> {
			started.countDown();
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
			}
			finished.countDown();
		});

		assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
		pool.shutdown();

		assertThat(finished.getCount()).isZero();
	}

	@Test
	void noNewSubmissionIsAcceptedAfterShutdown() {
		PaymentOperationWorkerPool pool = new PaymentOperationWorkerPool(properties(1));
		pool.shutdown();

		assertThatThrownBy(() -> pool.submit(() -> { }))
				.isInstanceOf(RejectedExecutionException.class);
	}

	private ProcessorProperties properties(int workerPoolSize) {
		return new ProcessorProperties(
				URI.create("http://localhost:8081"),
				Duration.ofSeconds(1),
				List.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20)),
				Duration.ofSeconds(3),
				Duration.ofSeconds(120),
				Duration.ofMillis(1),
				Duration.ofMillis(1),
				new ProcessorProperties.Reconciliation(
						Duration.ofSeconds(5), 20, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(60)),
				new ProcessorProperties.Workers(32, 8, 4, workerPoolSize));
	}
}
