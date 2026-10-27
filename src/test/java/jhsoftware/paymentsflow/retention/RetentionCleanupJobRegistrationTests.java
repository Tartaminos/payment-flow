package jhsoftware.paymentsflow.retention;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

class RetentionCleanupJobRegistrationTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(TestSupportConfiguration.class, RetentionConfiguration.class, RetentionCleanupJob.class)
			.withBean(RetentionCleanupRepository.class, () -> Mockito.mock(RetentionCleanupRepository.class))
			.withBean(Clock.class, Clock::systemUTC);

	@Test
	void jobDoesNotRegisterWhenCleanupIsNotEnabled() {
		contextRunner
				.withPropertyValues(
						"paymentflow.retention.cleanup.enabled=false",
						"paymentflow.retention.cleanup.dry-run=true",
						"paymentflow.retention.cleanup.financial-retention=P5Y",
						"paymentflow.retention.cleanup.cron=0 0 3 1 * *")
				.run(context -> assertThat(context).doesNotHaveBean(RetentionCleanupJob.class));
	}

	@Test
	void jobRegistersWhenCleanupIsExplicitlyEnabled() {
		contextRunner
				.withPropertyValues(
						"paymentflow.retention.cleanup.enabled=true",
						"paymentflow.retention.cleanup.dry-run=true",
						"paymentflow.retention.cleanup.financial-retention=P5Y",
						"paymentflow.retention.cleanup.cron=0 0 3 1 * *")
				.run(context -> assertThat(context).hasSingleBean(RetentionCleanupJob.class));
	}

	@EnableScheduling
	@Configuration(proxyBeanMethods = false)
	static class TestSupportConfiguration {
	}
}
