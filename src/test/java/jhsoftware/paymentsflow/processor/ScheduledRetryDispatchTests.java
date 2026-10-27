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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScheduledRetryDispatchTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private JdbcClient jdbcClient;
	private ScheduledRetryRepository repository;

	@BeforeAll
	void migrate() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(postgres.getJdbcUrl());
		config.setUsername(postgres.getUsername());
		config.setPassword(postgres.getPassword());
		dataSource = new HikariDataSource(config);
		Flyway.configure().dataSource(dataSource).load().migrate();
	}

	@AfterAll
	void closeDataSource() {
		dataSource.close();
	}

	@BeforeEach
	void setUp() {
		jdbcClient = JdbcClient.create(dataSource);
		repository = new ScheduledRetryRepository(jdbcClient, new RetryBackoffPolicy(() -> 0.5));
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM outbox_events").update();
		jdbcClient.sql("DELETE FROM scheduled_retries").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void dueScheduleIsClaimedAndDispatchedExactlyOnce() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID sourceMessageId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		UUID retryId = seedScheduledRetry(
				retryRow(operationId, correlationId, sourceMessageId, Instant.now().minusSeconds(5)));

		UUID workerId = UUID.randomUUID();
		Instant now = Instant.now();
		List<DueScheduledRetry> claimed = repository.claimDue(20, workerId, now, now.plusSeconds(60));

		assertThat(claimed).extracting(DueScheduledRetry::retryId).containsExactly(retryId);
		assertThat(stateOf(retryId)).isEqualTo("CLAIMED");

		repository.dispatch(claimed.get(0), workerId, now);

		assertThat(stateOf(retryId)).isEqualTo("DISPATCHED");
		List<OutboxRow> outboxRows = outboxRowsFor(operationId);
		assertThat(outboxRows).hasSize(1);
		OutboxRow outboxRow = outboxRows.get(0);
		assertThat(outboxRow.operationId()).isEqualTo(operationId);
		assertThat(outboxRow.correlationId()).isEqualTo(correlationId);
		assertThat(outboxRow.causationId()).isEqualTo(sourceMessageId);
		assertThat(outboxRow.paymentId()).isEqualTo(paymentId);
	}

	@Test
	void notYetDueScheduleIsNotClaimed() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		seedScheduledRetry(
				retryRow(operationId, UUID.randomUUID(), UUID.randomUUID(), Instant.now().plusSeconds(300)));

		List<DueScheduledRetry> claimed = repository.claimDue(20, UUID.randomUUID(), Instant.now(), Instant.now().plusSeconds(60));

		assertThat(claimed).isEmpty();
	}

	@Test
	void expiredClaimLeaseIsReclaimedAndSafelyRedispatched() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID sourceMessageId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		UUID retryId = seedScheduledRetry(
				retryRow(operationId, correlationId, sourceMessageId, Instant.now().minusSeconds(5)));

		UUID staleWorker = UUID.randomUUID();
		Instant past = Instant.now().minusSeconds(120);
		repository.claimDue(20, staleWorker, past, past.plusSeconds(60));

		UUID newWorker = UUID.randomUUID();
		Instant now = Instant.now();
		List<DueScheduledRetry> reclaimed = repository.claimDue(20, newWorker, now, now.plusSeconds(60));

		assertThat(reclaimed).extracting(DueScheduledRetry::retryId).containsExactly(retryId);
		repository.dispatch(reclaimed.get(0), newWorker, now);

		assertThat(outboxRowsFor(operationId)).hasSize(1);
	}

	@Test
	void lostClaimRollsBackAndDoesNotInsertADuplicateOutboxRow() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID sourceMessageId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		seedScheduledRetry(retryRow(operationId, correlationId, sourceMessageId, Instant.now().minusSeconds(5)));

		UUID staleWorker = UUID.randomUUID();
		Instant now = Instant.now();
		List<DueScheduledRetry> claimed = repository.claimDue(20, staleWorker, now, now.plusSeconds(60));
		UUID retryId = claimed.get(0).retryId();

		UUID newWorker = UUID.randomUUID();
		Instant later = now.plusSeconds(120);
		List<DueScheduledRetry> reclaimed = repository.claimDue(20, newWorker, later, later.plusSeconds(60));
		repository.dispatch(reclaimed.get(0), newWorker, later);

		TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
		assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(
				status -> repository.dispatch(claimed.get(0), staleWorker, now)))
				.isInstanceOf(LostScheduledRetryClaimException.class);

		assertThat(outboxRowsFor(operationId)).hasSize(1);
		assertThat(stateOf(retryId)).isEqualTo("DISPATCHED");
	}

	private RetryRow retryRow(UUID operationId, UUID correlationId, UUID sourceMessageId, Instant nextEligibleAt) {
		return new RetryRow(operationId, correlationId, sourceMessageId, nextEligibleAt);
	}

	private record RetryRow(UUID operationId, UUID correlationId, UUID sourceMessageId, Instant nextEligibleAt) {
	}

	private UUID seedScheduledRetry(RetryRow row) {
		UUID retryId = UUID.randomUUID();
		Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
		Instant scheduledAt = row.nextEligibleAt().isBefore(now) ? row.nextEligibleAt() : now;
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
				.param("operationId", row.operationId())
				.param("correlationId", row.correlationId())
				.param("sourceMessageId", row.sourceMessageId())
				.param("scheduledAt", scheduledAt.atOffset(ZoneOffset.UTC))
				.param("nextEligibleAt", row.nextEligibleAt().atOffset(ZoneOffset.UTC))
				.update();
		return retryId;
	}

	private String stateOf(UUID retryId) {
		return jdbcClient.sql("SELECT state FROM scheduled_retries WHERE retry_id = :retryId")
				.param("retryId", retryId)
				.query(String.class)
				.single();
	}

	private List<OutboxRow> outboxRowsFor(UUID operationId) {
		return jdbcClient.sql("""
				SELECT payment_id, operation_id, correlation_id, causation_id
				FROM outbox_events
				WHERE operation_id = :operationId
				""")
				.param("operationId", operationId)
				.query((resultSet, rowNumber) -> new OutboxRow(
						resultSet.getObject("payment_id", UUID.class),
						resultSet.getObject("operation_id", UUID.class),
						resultSet.getObject("correlation_id", UUID.class),
						resultSet.getObject("causation_id", UUID.class)))
				.list();
	}

	private record OutboxRow(UUID paymentId, UUID operationId, UUID correlationId, UUID causationId) {
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
}
