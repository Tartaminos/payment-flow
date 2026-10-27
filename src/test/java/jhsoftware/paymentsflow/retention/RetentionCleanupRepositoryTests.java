package jhsoftware.paymentsflow.retention;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Period;
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
class RetentionCleanupRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");
	private static final Instant OLD = NOW.atOffset(ZoneOffset.UTC).minus(Period.ofYears(6)).toInstant();
	private static final Instant RECENT = NOW.minusSeconds(60);
	private static final Period RETENTION = Period.ofYears(5);

	private static HikariDataSource dataSource;
	private JdbcClient jdbcClient;
	private RetentionCleanupRepository repository;

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
		repository = new RetentionCleanupRepository(jdbcClient);
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM quarantined_commands").update();
		jdbcClient.sql("DELETE FROM scheduled_retries").update();
		jdbcClient.sql("DELETE FROM reconciliation_escalations").update();
		jdbcClient.sql("DELETE FROM reconciliation_jobs").update();
		jdbcClient.sql("DELETE FROM outbox_events").update();
		jdbcClient.sql("DELETE FROM payment_audit_events").update();
		jdbcClient.sql("DELETE FROM idempotency_keys").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void oldFullyResolvedPaymentIsACleanupCandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).containsExactly(paymentId);
	}

	@Test
	void oldPaymentWithUnknownOperationIsNotACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "UNKNOWN");
		seedAuditEvent(paymentId, operationId, OLD);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).isEmpty();
	}

	@Test
	void oldPaymentWithOpenReconciliationJobIsNotACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "UNKNOWN");
		seedAuditEvent(paymentId, operationId, OLD);
		seedReconciliationJob(operationId, false);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).isEmpty();
	}

	@Test
	void oldPaymentWithOnlyResolvedReconciliationJobIsACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);
		seedReconciliationJob(operationId, true);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).containsExactly(paymentId);
	}

	@Test
	void oldPaymentWithOpenEscalationIsNotACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);
		UUID jobId = seedReconciliationJob(operationId, true);
		seedEscalation(operationId, jobId, "OPEN");

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).isEmpty();
	}

	@Test
	void oldPaymentWithOnlyClosedEscalationIsACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);
		UUID jobId = seedReconciliationJob(operationId, true);
		seedEscalation(operationId, jobId, "CLOSED");

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).containsExactly(paymentId);
	}

	@Test
	void oldPaymentWithActiveScheduledRetryIsNotACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);
		seedScheduledRetry(operationId);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).isEmpty();
	}

	@Test
	void oldPaymentWithQuarantinedCommandReferenceIsNotACandidate() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);
		seedQuarantinedCommand(operationId);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).isEmpty();
	}

	@Test
	void recentPaymentWithMixedOldAndRecentAuditRowsIsNotACandidateBecauseLatestRecordedAtIsRecent() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);
		seedAuditEvent(paymentId, operationId, RECENT);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);

		assertThat(candidates).isEmpty();
	}

	@Test
	void deleteCleanupCandidatesOnlyRemovesRowsWhenExplicitlyInvoked() {
		UUID paymentId = seedPayment();
		UUID operationId = seedOperation(paymentId, "SUCCEEDED");
		seedAuditEvent(paymentId, operationId, OLD);

		List<UUID> candidates = repository.findCleanupCandidatePaymentIds(NOW, RETENTION);
		assertThat(candidates).containsExactly(paymentId);
		assertThat(auditEventCount(paymentId)).isEqualTo(1);

		int deleted = repository.deleteCleanupCandidates(candidates);

		assertThat(deleted).isEqualTo(1);
		assertThat(auditEventCount(paymentId)).isZero();
		assertThat(paymentExists(paymentId)).isTrue();
	}

	private UUID seedPayment() {
		UUID paymentId = UUID.randomUUID();
		jdbcClient.sql("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (:id, :merchantId, :orderId, 5000, 'BRL', 'CAPTURED', 0, :now, :now)
				""")
				.param("id", paymentId)
				.param("merchantId", UUID.randomUUID())
				.param("orderId", "order-" + paymentId)
				.param("now", OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.update();
		return paymentId;
	}

	private UUID seedOperation(UUID paymentId, String status) {
		UUID operationId = UUID.randomUUID();
		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, 'AUTHORIZE', :status, 5000, :pspKey, 0, :now, :now)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("status", status)
				.param("pspKey", "psp-key-" + operationId)
				.param("now", OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.update();
		return operationId;
	}

	private void seedAuditEvent(UUID paymentId, UUID operationId, Instant recordedAt) {
		jdbcClient.sql("""
				INSERT INTO payment_audit_events
				    (event_id, payment_id, payment_operation_id, entity_type, event_type, previous_status, new_status,
				     reason_code, source, correlation_id, causation_id, sequence_number, metadata, occurred_at, recorded_at)
				VALUES
				    (:eventId, :paymentId, :operationId, 'PAYMENT_OPERATION', 'PAYMENT_OPERATION_CREATED', NULL, 'PENDING',
				     NULL, 'API', :correlationId, NULL, :sequenceNumber, '{}'::jsonb, :recordedAt, :recordedAt)
				""")
				.param("eventId", UUID.randomUUID())
				.param("paymentId", paymentId)
				.param("operationId", operationId)
				.param("correlationId", UUID.randomUUID())
				.param("sequenceNumber", nextSequence(paymentId))
				.param("recordedAt", OffsetDateTime.ofInstant(recordedAt, ZoneOffset.UTC))
				.update();
	}

	private long nextSequence(UUID paymentId) {
		return jdbcClient.sql("SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM payment_audit_events WHERE payment_id = :paymentId")
				.param("paymentId", paymentId)
				.query(Long.class)
				.single();
	}

	private UUID seedReconciliationJob(UUID operationId, boolean resolved) {
		UUID jobId = UUID.randomUUID();
		jdbcClient.sql("""
				INSERT INTO reconciliation_jobs
				    (id, operation_id, unknown_since, next_check_at, attempts, resolved_at, created_at, updated_at)
				VALUES
				    (:id, :operationId, :now, :now, 0, :resolvedAt, :now, :now)
				""")
				.param("id", jobId)
				.param("operationId", operationId)
				.param("now", OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.param("resolvedAt", resolved ? OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC) : null)
				.update();
		return jobId;
	}

	private void seedEscalation(UUID operationId, UUID jobId, String status) {
		jdbcClient.sql("""
				INSERT INTO reconciliation_escalations
				    (id, operation_id, reconciliation_job_id, reason, status, resolved_at, created_at, updated_at)
				VALUES
				    (:id, :operationId, :jobId, 'PSP_UNKNOWN_24H', :status, :resolvedAt, :now, :now)
				""")
				.param("id", UUID.randomUUID())
				.param("operationId", operationId)
				.param("jobId", jobId)
				.param("status", status)
				.param("resolvedAt", "OPEN".equals(status) ? null : OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.param("now", OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.update();
	}

	private void seedScheduledRetry(UUID operationId) {
		UUID retryId = UUID.randomUUID();
		jdbcClient.sql("""
				INSERT INTO scheduled_retries
				    (retry_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, source_schema_id,
				     failure_class, failed_at, operation_id, correlation_id, source_message_id,
				     retry_number, scheduled_at, next_eligible_at, state,
				     claim_generation, created_at, updated_at)
				VALUES
				    (:retryId, 'payment.operations.requested.v1', 0, :offset,
				     :sourceRecordKey, :sourceRecordValue, 1,
				     'DATA_ACCESS_UNAVAILABLE', :now, :operationId, :correlationId, :sourceMessageId,
				     1, :now, :now, 'PENDING',
				     0, :now, :now)
				""")
				.param("retryId", retryId)
				.param("offset", Math.abs(retryId.getLeastSignificantBits() % 1_000_000))
				.param("sourceRecordKey", new byte[] {1})
				.param("sourceRecordValue", new byte[] {1, 2, 3})
				.param("now", OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.param("operationId", operationId)
				.param("correlationId", UUID.randomUUID())
				.param("sourceMessageId", UUID.randomUUID())
				.update();
	}

	private void seedQuarantinedCommand(UUID operationId) {
		jdbcClient.sql("""
				INSERT INTO quarantined_commands
				    (quarantine_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, failure_code, quarantined_at,
				     operation_id, correlation_id, source_message_id, created_at)
				VALUES
				    (:quarantineId, 'payment.operations.requested.v1', 0, :offset,
				     :sourceRecordKey, :sourceRecordValue, 'OPERATION_NOT_FOUND', :now,
				     :operationId, :correlationId, :sourceMessageId, :now)
				""")
				.param("quarantineId", UUID.randomUUID())
				.param("offset", Math.abs(operationId.getLeastSignificantBits() % 1_000_000))
				.param("sourceRecordKey", new byte[] {1})
				.param("sourceRecordValue", new byte[] {1, 2, 3})
				.param("now", OffsetDateTime.ofInstant(OLD, ZoneOffset.UTC))
				.param("operationId", operationId)
				.param("correlationId", UUID.randomUUID())
				.param("sourceMessageId", UUID.randomUUID())
				.update();
	}

	private long auditEventCount(UUID paymentId) {
		return jdbcClient.sql("SELECT COUNT(*) FROM payment_audit_events WHERE payment_id = :paymentId")
				.param("paymentId", paymentId)
				.query(Long.class)
				.single();
	}

	private boolean paymentExists(UUID paymentId) {
		return jdbcClient.sql("SELECT EXISTS (SELECT 1 FROM payments WHERE id = :paymentId)")
				.param("paymentId", paymentId)
				.query(Boolean.class)
				.single();
	}
}
