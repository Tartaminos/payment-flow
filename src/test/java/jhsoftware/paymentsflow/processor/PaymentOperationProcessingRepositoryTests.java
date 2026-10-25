package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
class PaymentOperationProcessingRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private PaymentOperationProcessingRepository repository;
	private JdbcClient jdbcClient;

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
		repository = new PaymentOperationProcessingRepository(jdbcClient);
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM payment_audit_events").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void succeededOutcomeAuthorizesPaymentAndAppendsOrderedAudit() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId, "PENDING", "CREATED");

		boolean ackEligible = repository.commitOutcome(
				operationId,
				PspAuthorizationOutcome.succeeded(operationId.toString()),
				UUID.randomUUID(),
				UUID.randomUUID(),
				Instant.parse("2026-10-25T10:00:00Z"));

		assertThat(ackEligible).isTrue();
		assertThat(operationStatus(operationId)).isEqualTo("SUCCEEDED");
		assertThat(paymentStatus(paymentId)).isEqualTo("AUTHORIZED");
		assertThat(auditEventTypesInOrder(paymentId)).containsExactly(
				"PAYMENT_OPERATION_SUCCEEDED", "PAYMENT_AUTHORIZED");
	}

	@Test
	void rejectedOutcomeDeclinesPayment() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId, "PENDING", "CREATED");

		boolean ackEligible = repository.commitOutcome(
				operationId,
				PspAuthorizationOutcome.rejected(operationId.toString(), "CARD_DECLINED"),
				UUID.randomUUID(),
				null,
				Instant.parse("2026-10-25T10:00:00Z"));

		assertThat(ackEligible).isTrue();
		assertThat(operationStatus(operationId)).isEqualTo("REJECTED");
		assertThat(paymentStatus(paymentId)).isEqualTo("DECLINED");
	}

	@Test
	void failedOutcomeFinalizesOperationButLeavesPaymentStatusUnchanged() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId, "PENDING", "CREATED");

		boolean ackEligible = repository.commitOutcome(
				operationId,
				PspAuthorizationOutcome.failed(operationId.toString(), "NON_EXECUTION_CONFIRMED"),
				UUID.randomUUID(),
				null,
				Instant.parse("2026-10-25T10:00:00Z"));

		assertThat(ackEligible).isTrue();
		assertThat(operationStatus(operationId)).isEqualTo("FAILED");
		assertThat(paymentStatus(paymentId)).isEqualTo("CREATED");
		assertThat(auditEventTypesInOrder(paymentId)).containsExactly("PAYMENT_OPERATION_FAILED");
	}

	@Test
	void unknownOutcomeLeavesPaymentUnchangedAndWithholdsAcknowledgment() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId, "PENDING", "CREATED");

		boolean ackEligible = repository.commitOutcome(
				operationId,
				PspAuthorizationOutcome.unknown("PSP_UNREACHABLE"),
				UUID.randomUUID(),
				null,
				Instant.parse("2026-10-25T10:00:00Z"));

		assertThat(ackEligible).isFalse();
		assertThat(operationStatus(operationId)).isEqualTo("UNKNOWN");
		assertThat(paymentStatus(paymentId)).isEqualTo("CREATED");
		assertThat(auditEventTypesInOrder(paymentId)).containsExactly("PAYMENT_OPERATION_UNKNOWN");
	}

	@Test
	void alreadyFinalizedOperationIsAcknowledgedWithoutAnyFurtherEffect() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId, "SUCCEEDED", "AUTHORIZED");

		boolean ackEligible = repository.commitOutcome(
				operationId,
				PspAuthorizationOutcome.rejected(operationId.toString(), "SHOULD_NOT_APPLY"),
				UUID.randomUUID(),
				null,
				Instant.parse("2026-10-25T10:00:00Z"));

		assertThat(ackEligible).isTrue();
		assertThat(operationStatus(operationId)).isEqualTo("SUCCEEDED");
		assertThat(paymentStatus(paymentId)).isEqualTo("AUTHORIZED");
		assertThat(auditEventTypesInOrder(paymentId)).isEmpty();
	}

	@Test
	void loadContextJoinsParentPayment() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId, "PENDING", "CREATED");

		PaymentOperationContext context = repository.loadContext(operationId).orElseThrow();

		assertThat(context.operationId()).isEqualTo(operationId);
		assertThat(context.paymentId()).isEqualTo(paymentId);
		assertThat(context.operationType()).isEqualTo("AUTHORIZE");
		assertThat(context.operationStatus()).isEqualTo("PENDING");
		assertThat(context.amountMinor()).isEqualTo(50000L);
		assertThat(context.currency()).isEqualTo("BRL");
	}

	private void seedPaymentAndOperation(
			UUID paymentId, UUID operationId, String operationStatus, String paymentStatus) {
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		jdbcClient.sql("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (:id, :merchantId, :orderId, 50000, 'BRL', :status, 0, :now, :now)
				""")
				.param("id", paymentId)
				.param("merchantId", UUID.randomUUID())
				.param("orderId", "order-" + paymentId)
				.param("status", paymentStatus)
				.param("now", now)
				.update();

		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, 'AUTHORIZE', :status, 50000, :pspIdempotencyKey, 0, :now, :now)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("status", operationStatus)
				.param("pspIdempotencyKey", operationId.toString())
				.param("now", now)
				.update();
	}

	private String operationStatus(UUID operationId) {
		return jdbcClient.sql("SELECT status FROM payment_operations WHERE id = :id")
				.param("id", operationId)
				.query(String.class)
				.single();
	}

	private String paymentStatus(UUID paymentId) {
		return jdbcClient.sql("SELECT status FROM payments WHERE id = :id")
				.param("id", paymentId)
				.query(String.class)
				.single();
	}

	private java.util.List<String> auditEventTypesInOrder(UUID paymentId) {
		return jdbcClient.sql("""
				SELECT event_type FROM payment_audit_events
				WHERE payment_id = :paymentId
				ORDER BY sequence_number
				""")
				.param("paymentId", paymentId)
				.query(String.class)
				.list();
	}
}
