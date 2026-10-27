package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringJUnitConfig(ScheduledRetryRepositoryTransactionalProxyTests.Config.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ScheduledRetryRepositoryTransactionalProxyTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private ScheduledRetryRepository repository;

	@Autowired
	private DataSource dataSource;

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM outbox_events").update();
		jdbcClient.sql("DELETE FROM scheduled_retries").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void lostClaimRollsBackThroughTheRealTransactionalProxyAndDoesNotInsertAnOutboxRow() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID sourceMessageId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		UUID retryId = seedScheduledRetry(operationId, correlationId, sourceMessageId, Instant.now().minusSeconds(5));

		UUID workerId = UUID.randomUUID();
		Instant now = Instant.now();
		List<DueScheduledRetry> claimed = repository.claimDue(20, workerId, now, now.plusSeconds(60));
		assertThat(claimed).extracting(DueScheduledRetry::retryId).containsExactly(retryId);

		UUID mismatchedWorker = UUID.randomUUID();
		assertThatThrownBy(() -> repository.dispatch(claimed.get(0), mismatchedWorker, now))
				.isInstanceOf(LostScheduledRetryClaimException.class);

		assertThat(outboxRowCountFor(operationId)).isZero();
		assertThat(stateOf(retryId)).isEqualTo("CLAIMED");
		assertThat(claimOwnerOf(retryId)).isEqualTo(workerId);
	}

	private UUID seedScheduledRetry(
			UUID operationId, UUID correlationId, UUID sourceMessageId, Instant nextEligibleAt) {
		UUID retryId = UUID.randomUUID();
		Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
		Instant scheduledAt = nextEligibleAt.isBefore(now) ? nextEligibleAt : now;
		jdbcClient.sql("""
				INSERT INTO scheduled_retries
				    (retry_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, source_schema_id,
				     failure_class, failed_at, operation_id, correlation_id, source_message_id,
				     retry_number, scheduled_at, next_eligible_at, state,
				     claim_generation, created_at, updated_at)
				VALUES
				    (:retryId, 'payment.operations.requested.v1', 1, :offset,
				     :sourceRecordKey, :sourceRecordValue, 1,
				     'DATA_ACCESS_UNAVAILABLE', :now, :operationId, :correlationId, :sourceMessageId,
				     1, :scheduledAt, :nextEligibleAt, 'PENDING',
				     0, :now, :now)
				""")
				.param("retryId", retryId)
				.param("offset", Math.abs(retryId.getLeastSignificantBits() % 1_000_000))
				.param("sourceRecordKey", new byte[] {1})
				.param("sourceRecordValue", new byte[] {1, 2, 3})
				.param("now", now.atOffset(ZoneOffset.UTC))
				.param("operationId", operationId)
				.param("correlationId", correlationId)
				.param("sourceMessageId", sourceMessageId)
				.param("scheduledAt", scheduledAt.atOffset(ZoneOffset.UTC))
				.param("nextEligibleAt", nextEligibleAt.atOffset(ZoneOffset.UTC))
				.update();
		return retryId;
	}

	private String stateOf(UUID retryId) {
		return jdbcClient.sql("SELECT state FROM scheduled_retries WHERE retry_id = :retryId")
				.param("retryId", retryId)
				.query(String.class)
				.single();
	}

	private UUID claimOwnerOf(UUID retryId) {
		return jdbcClient.sql("SELECT claim_owner FROM scheduled_retries WHERE retry_id = :retryId")
				.param("retryId", retryId)
				.query(UUID.class)
				.single();
	}

	private long outboxRowCountFor(UUID operationId) {
		return jdbcClient.sql("SELECT count(*) FROM outbox_events WHERE operation_id = :operationId")
				.param("operationId", operationId)
				.query(Long.class)
				.single();
	}

	private void seedPaymentAndOperation(UUID paymentId, UUID operationId) {
		Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement payment = connection.prepareStatement("""
					INSERT INTO payments
					    (id, merchant_id, external_order_id, amount_minor, currency, status, created_at, updated_at)
					VALUES (?, ?, ?, 5000, 'BRL', 'CREATED', ?, ?)
					""")) {
				payment.setObject(1, paymentId);
				payment.setObject(2, UUID.randomUUID());
				payment.setString(3, "order-" + paymentId);
				payment.setObject(4, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				payment.setObject(5, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				payment.executeUpdate();
			}
			try (PreparedStatement operation = connection.prepareStatement("""
					INSERT INTO payment_operations
					    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count,
					     created_at, updated_at)
					VALUES (?, ?, 'AUTHORIZE', 'PENDING', 5000, ?, 0, ?, ?)
					""")) {
				operation.setObject(1, operationId);
				operation.setObject(2, paymentId);
				operation.setString(3, "psp-key-" + operationId);
				operation.setObject(4, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				operation.setObject(5, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				operation.executeUpdate();
			}
		}
		catch (Exception exception) {
			throw new RuntimeException(exception);
		}
	}

	@Configuration
	@EnableTransactionManagement
	static class Config {

		@Bean
		DataSource dataSource() {
			HikariConfig config = new HikariConfig();
			config.setJdbcUrl(postgres.getJdbcUrl());
			config.setUsername(postgres.getUsername());
			config.setPassword(postgres.getPassword());
			HikariDataSource dataSource = new HikariDataSource(config);
			Flyway.configure().dataSource(dataSource).load().migrate();
			return dataSource;
		}

		@Bean
		PlatformTransactionManager transactionManager(DataSource dataSource) {
			return new DataSourceTransactionManager(dataSource);
		}

		@Bean
		JdbcClient jdbcClient(DataSource dataSource) {
			return JdbcClient.create(dataSource);
		}

		@Bean
		RetryBackoffPolicy retryBackoffPolicy() {
			return new RetryBackoffPolicy(() -> 0.5);
		}

		@Bean
		ScheduledRetryRepository scheduledRetryRepository(JdbcClient jdbcClient, RetryBackoffPolicy backoffPolicy) {
			return new ScheduledRetryRepository(jdbcClient, backoffPolicy);
		}
	}
}
