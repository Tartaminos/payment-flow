package jhsoftware.paymentsflow.processor.repository;

import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcome;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
class ReconciliationJobRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private JdbcClient jdbcClient;
	private PaymentOperationProcessingRepository operationRepository;
	private ReconciliationEscalationRepository escalationRepository;
	private ReconciliationJobRepository repository;

	@BeforeAll
	void migrate() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(postgres.getJdbcUrl());
		config.setUsername(postgres.getUsername());
		config.setPassword(postgres.getPassword());
		config.setMaximumPoolSize(16);
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
		operationRepository = new PaymentOperationProcessingRepository(jdbcClient);
		escalationRepository = new ReconciliationEscalationRepository(jdbcClient);
		repository = new ReconciliationJobRepository(jdbcClient, operationRepository, escalationRepository);
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
	void claimDueJobsOnlyReturnsJobsThatAreDueAndUnresolved() {
		UUID paymentId = UUID.randomUUID();
		UUID dueOperationId = UUID.randomUUID();
		UUID notDueOperationId = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, dueOperationId, "UNKNOWN", "CREATED");
		seedPaymentAndOperation(paymentId, notDueOperationId, "UNKNOWN", "CREATED");
		insertJob(dueOperationId, now.minusSeconds(10));
		insertJob(notDueOperationId, now.plusSeconds(60));

		List<ReconciliationJobRepository.ReconciliationJobClaim> claims =
				repository.claimDueJobs(UUID.randomUUID(), now, 20, Duration.ofSeconds(30));

		assertThat(claims).extracting(ReconciliationJobRepository.ReconciliationJobClaim::operationId)
				.containsExactly(dueOperationId);
	}

	@Test
	void resolveJobAppliesDefinitiveOutcomeAndClearsTheJob() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		assertThat(operationLeaseOwner(operationId)).isNull();
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		List<ReconciliationJobRepository.ReconciliationJobClaim> claims =
				repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));
		assertThat(claims).hasSize(1);

		ReconciliationJobRepository.ResolutionOutcome resolution = repository.resolveJob(
				jobId, self, operationId, PspAuthorizationOutcome.succeeded(operationId.toString()),
				UUID.randomUUID(), null, now);

		assertThat(resolution).isEqualTo(ReconciliationJobRepository.ResolutionOutcome.RESOLVED);
		assertThat(operationStatus(operationId)).isEqualTo("SUCCEEDED");
		assertThat(paymentStatus(paymentId)).isEqualTo("AUTHORIZED");
		assertThat(jobResolvedAt(jobId)).isNotNull();
	}

	@Test
	void resolveJobClosesOpenEscalationsAndAppendsExactlyOneAuditRowPerClosedReason() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));
		escalationRepository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);

		ReconciliationJobRepository.ResolutionOutcome resolution = repository.resolveJob(
				jobId, self, operationId, PspAuthorizationOutcome.succeeded(operationId.toString()),
				UUID.randomUUID(), null, now);

		assertThat(resolution).isEqualTo(ReconciliationJobRepository.ResolutionOutcome.RESOLVED);
		assertThat(escalationStatus(operationId, "PSP_UNKNOWN_24H")).isEqualTo("CLOSED");
		assertThat(auditEventTypesInOrder(paymentId)).containsExactly(
				"PAYMENT_OPERATION_SUCCEEDED", "PAYMENT_AUTHORIZED", "RECONCILIATION_ESCALATION_CLOSED");
		assertThat(operationStatus(operationId)).isEqualTo("SUCCEEDED");
		assertThat(paymentStatus(paymentId)).isNotIn("FAILED", "CANCELED");
	}

	@Test
	void bumpBackoffLeavesExistingEscalationsOpenWhenTheOperationStaysUnknown() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));
		escalationRepository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);

		boolean bumped = repository.bumpBackoff(jobId, self, "PSP_LOOKUP_NOT_FOUND", now, now.plusSeconds(10));

		assertThat(bumped).isTrue();
		assertThat(escalationStatus(operationId, "PSP_UNKNOWN_24H")).isEqualTo("OPEN");
		assertThat(operationStatus(operationId)).isEqualTo("UNKNOWN");
	}

	@Test
	void flagContradictionCreatesItsOwnEscalationWithoutTouchingAnExistingUnknown24hRow() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));
		escalationRepository.ensureEscalation(operationId, jobId, "PSP_UNKNOWN_24H", now);

		boolean flagged = repository.flagContradiction(
				jobId, self, operationId, now, now.plusSeconds(10), "PSP_EVIDENCE_CONTRADICTION");

		assertThat(flagged).isTrue();
		assertThat(escalationStatus(operationId, "PSP_UNKNOWN_24H")).isEqualTo("OPEN");
		assertThat(escalationStatus(operationId, "PSP_EVIDENCE_CONTRADICTION")).isEqualTo("OPEN");
		assertThat(escalationCount(operationId)).isEqualTo(2);
	}

	@Test
	void resolveJobDetectsContradictionAgainstAnAlreadyFinalizedOperationWithoutClosingTheJob() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "SUCCEEDED", "AUTHORIZED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));

		ReconciliationJobRepository.ResolutionOutcome resolution = repository.resolveJob(
				jobId, self, operationId, PspAuthorizationOutcome.rejected(operationId.toString(), "CARD_DECLINED"),
				UUID.randomUUID(), null, now);

		assertThat(resolution).isEqualTo(ReconciliationJobRepository.ResolutionOutcome.CONTRADICTION);
		assertThat(operationStatus(operationId)).isEqualTo("SUCCEEDED");
		assertThat(jobResolvedAt(jobId)).isNull();
	}

	@Test
	void flagContradictionRecordsTheReasonBumpsBackoffAndLeavesTheJobUnresolvedAndVisible() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));

		boolean flagged = repository.flagContradiction(
				jobId, self, operationId, now, now.plusSeconds(10), "PSP_EVIDENCE_CONTRADICTION");

		assertThat(flagged).isTrue();
		assertThat(jobResolvedAt(jobId)).isNull();
		assertThat(jobLeaseOwner(jobId)).isNull();
		assertThat(jobLastError(jobId)).isEqualTo("PSP_EVIDENCE_CONTRADICTION");
		assertThat(jobAttempts(jobId)).isEqualTo(1);
		assertThat(jobNextCheckAt(jobId)).isEqualTo(now.plusSeconds(10).atOffset(ZoneOffset.UTC));
	}

	@Test
	void flagContradictionFailsWhenTheLeaseIsNotOwnedBySelf() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(UUID.randomUUID(), now, 20, Duration.ofSeconds(30));

		boolean flagged = repository.flagContradiction(
				jobId, UUID.randomUUID(), operationId, now, now.plusSeconds(10), "PSP_EVIDENCE_CONTRADICTION");

		assertThat(flagged).isFalse();
	}

	@Test
	void contradictedJobIsNotImmediatelyReclaimableAndBecomesDueAfterBackoff() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));
		Instant nextCheckAt = now.plusSeconds(10);

		boolean flagged =
				repository.flagContradiction(jobId, self, operationId, now, nextCheckAt, "PSP_EVIDENCE_CONTRADICTION");
		assertThat(flagged).isTrue();

		List<ReconciliationJobRepository.ReconciliationJobClaim> immediateReclaim =
				repository.claimDueJobs(UUID.randomUUID(), now, 20, Duration.ofSeconds(30));
		assertThat(immediateReclaim).isEmpty();

		List<ReconciliationJobRepository.ReconciliationJobClaim> tooEarlyReclaim =
				repository.claimDueJobs(UUID.randomUUID(), nextCheckAt.minusSeconds(1), 20, Duration.ofSeconds(30));
		assertThat(tooEarlyReclaim).isEmpty();

		List<ReconciliationJobRepository.ReconciliationJobClaim> reclaimAfterBackoff =
				repository.claimDueJobs(UUID.randomUUID(), nextCheckAt, 20, Duration.ofSeconds(30));
		assertThat(reclaimAfterBackoff).extracting(ReconciliationJobRepository.ReconciliationJobClaim::jobId)
				.containsExactly(jobId);
	}

	@Test
	void bumpBackoffIncrementsAttemptsAndReleasesTheLease() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID self = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
		UUID jobId = insertJob(operationId, now.minusSeconds(10));
		repository.claimDueJobs(self, now, 20, Duration.ofSeconds(30));

		boolean bumped = repository.bumpBackoff(jobId, self, "PSP_LOOKUP_NOT_FOUND", now, now.plusSeconds(10));

		assertThat(bumped).isTrue();
		assertThat(jobAttempts(jobId)).isEqualTo(1);
		assertThat(jobLeaseOwner(jobId)).isNull();
	}

	@Test
	void concurrentScannersNeverDoubleClaimTheSameJob() throws Exception {
		UUID paymentId = UUID.randomUUID();
		Instant now = Instant.parse("2026-10-25T10:00:00Z");
		int jobCount = 20;
		for (int i = 0; i < jobCount; i++) {
			UUID operationId = UUID.randomUUID();
			seedPaymentAndOperation(paymentId, operationId, "UNKNOWN", "CREATED");
			insertJob(operationId, now.minusSeconds(10));
		}

		int scannerCount = 6;
		CyclicBarrier barrier = new CyclicBarrier(scannerCount);
		ExecutorService executor = Executors.newFixedThreadPool(scannerCount);
		List<Future<List<ReconciliationJobRepository.ReconciliationJobClaim>>> futures = new ArrayList<>();
		for (int i = 0; i < scannerCount; i++) {
			UUID scannerId = UUID.randomUUID();
			futures.add(executor.submit(() -> {
				barrier.await();
				return repository.claimDueJobs(scannerId, now, jobCount, Duration.ofSeconds(30));
			}));
		}

		List<UUID> claimedJobIds = new ArrayList<>();
		for (Future<List<ReconciliationJobRepository.ReconciliationJobClaim>> future : futures) {
			claimedJobIds.addAll(future.get(10, TimeUnit.SECONDS).stream()
					.map(ReconciliationJobRepository.ReconciliationJobClaim::jobId)
					.collect(Collectors.toList()));
		}
		executor.shutdown();

		assertThat(claimedJobIds).hasSize(jobCount);
		assertThat(claimedJobIds).doesNotHaveDuplicates();
	}

	private UUID insertJob(UUID operationId, Instant nextCheckAt) {
		UUID jobId = UUID.randomUUID();
		jdbcClient.sql("""
				INSERT INTO reconciliation_jobs
				    (id, operation_id, unknown_since, next_check_at, attempts, created_at, updated_at)
				VALUES
				    (:id, :operationId, :now, :nextCheckAt, 0, :now, :now)
				""")
				.param("id", jobId)
				.param("operationId", operationId)
				.param("now", OffsetDateTime.now(ZoneOffset.UTC))
				.param("nextCheckAt", nextCheckAt.atOffset(ZoneOffset.UTC))
				.update();
		return jobId;
	}

	private void seedPaymentAndOperation(
			UUID paymentId, UUID operationId, String operationStatus, String paymentStatus) {
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		if (!paymentExists(paymentId)) {
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
		}

		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, 'AUTHORIZE', :status, 50000, :pspIdempotencyKey, 3, :now, :now)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("status", operationStatus)
				.param("pspIdempotencyKey", operationId.toString())
				.param("now", now)
				.update();
	}

	private boolean paymentExists(UUID paymentId) {
		return jdbcClient.sql("SELECT COUNT(*) FROM payments WHERE id = :id")
				.param("id", paymentId)
				.query(Long.class)
				.single() > 0;
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

	private List<String> auditEventTypesInOrder(UUID paymentId) {
		return jdbcClient.sql("""
				SELECT event_type FROM payment_audit_events
				WHERE payment_id = :paymentId
				ORDER BY sequence_number
				""")
				.param("paymentId", paymentId)
				.query(String.class)
				.list();
	}

	private OffsetDateTime jobResolvedAt(UUID jobId) {
		return jdbcClient.sql("SELECT resolved_at FROM reconciliation_jobs WHERE id = :id")
				.param("id", jobId)
				.query(OffsetDateTime.class)
				.optional()
				.orElse(null);
	}

	private int jobAttempts(UUID jobId) {
		return jdbcClient.sql("SELECT attempts FROM reconciliation_jobs WHERE id = :id")
				.param("id", jobId)
				.query(Integer.class)
				.single();
	}

	private OffsetDateTime jobNextCheckAt(UUID jobId) {
		return jdbcClient.sql("SELECT next_check_at FROM reconciliation_jobs WHERE id = :id")
				.param("id", jobId)
				.query(OffsetDateTime.class)
				.single();
	}

	private UUID operationLeaseOwner(UUID operationId) {
		return jdbcClient.sql("SELECT lease_owner FROM payment_operations WHERE id = :id")
				.param("id", operationId)
				.query(UUID.class)
				.optional()
				.orElse(null);
	}

	private String jobLastError(UUID jobId) {
		return jdbcClient.sql("SELECT last_error FROM reconciliation_jobs WHERE id = :id")
				.param("id", jobId)
				.query(String.class)
				.optional()
				.orElse(null);
	}

	private UUID jobLeaseOwner(UUID jobId) {
		return jdbcClient.sql("SELECT lease_owner FROM reconciliation_jobs WHERE id = :id")
				.param("id", jobId)
				.query(UUID.class)
				.optional()
				.orElse(null);
	}
}
