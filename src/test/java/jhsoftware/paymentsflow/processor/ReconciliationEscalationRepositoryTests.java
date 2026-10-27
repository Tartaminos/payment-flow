package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReconciliationEscalationRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private JdbcClient jdbcClient;
	private ReconciliationEscalationRepository repository;

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
		repository = new ReconciliationEscalationRepository(jdbcClient);
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM payment_audit_events").update();
		jdbcClient.sql("DELETE FROM reconciliation_escalations").update();
		jdbcClient.sql("DELETE FROM reconciliation_jobs").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void ensureEscalationCreatesAnOpenRowOnFirstCall() {
		UUID operationId = UUID.randomUUID();
		UUID jobId = seedPaymentOperationAndJob(operationId);
		Instant now = Instant.parse("2026-10-25T10:00:00Z");

		boolean created = repository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);

		assertThat(created).isTrue();
		assertThat(escalationStatus(operationId, "PSP_UNKNOWN_24H")).isEqualTo("OPEN");
	}

	@Test
	void ensureEscalationIsIdempotentForTheSameOperationAndReason() {
		UUID operationId = UUID.randomUUID();
		UUID jobId = seedPaymentOperationAndJob(operationId);
		Instant now = Instant.parse("2026-10-25T10:00:00Z");

		boolean firstCreated = repository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);
		boolean secondCreated = repository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now.plusSeconds(5));

		assertThat(firstCreated).isTrue();
		assertThat(secondCreated).isFalse();
		assertThat(escalationCount(operationId)).isEqualTo(1);
	}

	@Test
	void ensureEscalationAllowsTwoSimultaneousOpenRowsForDifferentReasons() {
		UUID operationId = UUID.randomUUID();
		UUID jobId = seedPaymentOperationAndJob(operationId);
		Instant now = Instant.parse("2026-10-25T10:00:00Z");

		boolean unknownCreated = repository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);
		boolean contradictionCreated = repository.ensureEscalation(operationId, jobId, "PSP_EVIDENCE_CONTRADICTION", now);

		assertThat(unknownCreated).isTrue();
		assertThat(contradictionCreated).isTrue();
		assertThat(escalationCount(operationId)).isEqualTo(2);
	}

	@Test
	void closeOpenEscalationsClosesEveryOpenRowAndReturnsTheirReasons() {
		UUID operationId = UUID.randomUUID();
		UUID jobId = seedPaymentOperationAndJob(operationId);
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		repository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);
		repository.ensureEscalation(operationId, jobId, "PSP_EVIDENCE_CONTRADICTION", now);

		List<String> closedReasons = repository.closeOpenEscalations(operationId, now.plusSeconds(10));

		assertThat(closedReasons).containsExactlyInAnyOrder("PSP_UNKNOWN_24H", "PSP_EVIDENCE_CONTRADICTION");
		assertThat(escalationStatus(operationId, "PSP_UNKNOWN_24H")).isEqualTo("CLOSED");
		assertThat(escalationStatus(operationId, "PSP_EVIDENCE_CONTRADICTION")).isEqualTo("CLOSED");
	}

	@Test
	void closeOpenEscalationsReturnsNothingWhenNoneAreOpen() {
		UUID operationId = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");

		List<String> closedReasons = repository.closeOpenEscalations(operationId, now);

		assertThat(closedReasons).isEmpty();
	}

	private UUID seedPaymentOperationAndJob(UUID operationId) {
		UUID paymentId = UUID.randomUUID();
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		jdbcClient.sql("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (:id, :merchantId, :orderId, 50000, 'BRL', 'CREATED', 0, :now, :now)
				""")
				.param("id", paymentId)
				.param("merchantId", UUID.randomUUID())
				.param("orderId", "order-" + paymentId)
				.param("now", now)
				.update();

		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, 'AUTHORIZE', 'UNKNOWN', 50000, :pspIdempotencyKey, 3, :now, :now)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("pspIdempotencyKey", operationId.toString())
				.param("now", now)
				.update();

		UUID jobId = UUID.randomUUID();
		jdbcClient.sql("""
				INSERT INTO reconciliation_jobs
				    (id, operation_id, unknown_since, next_check_at, attempts, created_at, updated_at)
				VALUES
				    (:id, :operationId, :now, :now, 0, :now, :now)
				""")
				.param("id", jobId)
				.param("operationId", operationId)
				.param("now", now)
				.update();
		return jobId;
	}

	private String escalationStatus(UUID operationId, String reason) {
		return jdbcClient.sql("SELECT status FROM reconciliation_escalations WHERE operation_id = :operationId AND reason = :reason")
				.param("operationId", operationId)
				.param("reason", reason)
				.query(String.class)
				.single();
	}

	private long escalationCount(UUID operationId) {
		return jdbcClient.sql("SELECT COUNT(*) FROM reconciliation_escalations WHERE operation_id = :operationId")
				.param("operationId", operationId)
				.query(Long.class)
				.single();
	}
}
