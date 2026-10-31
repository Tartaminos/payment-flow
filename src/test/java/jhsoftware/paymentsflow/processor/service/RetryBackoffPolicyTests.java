package jhsoftware.paymentsflow.processor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class RetryBackoffPolicyTests {

	@Test
	void appliesDeterministicLowerAndUpperJitterBounds() {
		assertThat(new RetryBackoffPolicy(() -> 0.0).delay(1)).isEqualTo(Duration.ofSeconds(8));
		assertThat(new RetryBackoffPolicy(() -> 0.999999).delay(2).toMillis()).isEqualTo(24_000L);
	}

	@Test
	void capsSixthAndLaterRetriesAtFiveMinutes() {
		assertThat(new RetryBackoffPolicy(() -> 0.0).delay(6)).isEqualTo(Duration.ofSeconds(240));
		assertThat(new RetryBackoffPolicy(() -> 0.999999).delay(6)).isEqualTo(Duration.ofSeconds(300));
		assertThat(new RetryBackoffPolicy(() -> 0.5).delay(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(300));
	}

	@Test
	void rejectsInvalidRetryNumberAndRandomSource() {
		assertThatThrownBy(() -> new RetryBackoffPolicy(() -> 0.5).delay(0))
				.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new RetryBackoffPolicy(() -> 1.0).delay(1))
				.isInstanceOf(IllegalStateException.class);
	}
}
