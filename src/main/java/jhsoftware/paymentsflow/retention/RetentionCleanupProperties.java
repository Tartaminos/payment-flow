package jhsoftware.paymentsflow.retention;

import java.time.Period;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paymentflow.retention")
record RetentionCleanupProperties(Cleanup cleanup) {

	RetentionCleanupProperties {
		if (cleanup == null) {
			throw new IllegalArgumentException("paymentflow.retention.cleanup is required");
		}
	}

	record Cleanup(boolean enabled, boolean dryRun, Period financialRetention, String cron) {

		Cleanup {
			if (financialRetention == null || financialRetention.isZero() || financialRetention.isNegative()) {
				throw new IllegalArgumentException("paymentflow.retention.cleanup.financial-retention must be positive");
			}
			if (cron == null || cron.isBlank()) {
				throw new IllegalArgumentException("paymentflow.retention.cleanup.cron is required");
			}
		}
	}
}
