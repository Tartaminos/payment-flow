package jhsoftware.paymentsflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
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
class CaptureAdmissionRepositoryTests {

	private static final UUID MERCHANT_ID = UUID.randomUUID();
	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");
	private static final String HASH_1 = "1".repeat(64);
	private static final String HASH_A = "a".repeat(64);
	private static final String HASH_B = "b".repeat(64);

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private CaptureAdmissionRepository repository;
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
		repository = new CaptureAdmissionRepository(jdbcClient);
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM outbox_events").update();
		jdbcClient.sql("DELETE FROM payment_audit_events").update();
		jdbcClient.sql("DELETE FROM idempotency_keys").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void findCaptureDerivationReturnsAmountCurrencyAndAuthorizationPspReference() {
		UUID paymentId = UUID.randomUUID();
		UUID authorizeOperationId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(authorizeOperationId, paymentId, "AUTHORIZE", "SUCCEEDED", authorizeOperationId.toString());

		var derivation = repository.findCaptureDerivation(MERCHANT_ID, paymentId).orElseThrow();

		assertThat(derivation.amountMinor()).isEqualTo(50_000L);
		assertThat(derivation.currency()).isEqualTo("BRL");
		assertThat(derivation.authorizationPspReference()).isEqualTo(authorizeOperationId.toString());
	}

	@Test
	void findCaptureDerivationForUnknownPaymentIsEmpty() {
		assertThat(repository.findCaptureDerivation(MERCHANT_ID, UUID.randomUUID())).isEmpty();
	}

	@Test
	void findCaptureDerivationWithoutAuthorizeOperationHasNullPspReference() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CREATED", 10_000L, "USD");

		var derivation = repository.findCaptureDerivation(MERCHANT_ID, paymentId).orElseThrow();

		assertThat(derivation.authorizationPspReference()).isNull();
	}

	@Test
	void firstAdmissionInsertsCaptureOperationIdempotencyAuditAndOutbox() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "capture-key-1");
		CreatePaymentResponse response = repository.admitCapture(MERCHANT_ID, command, HASH_1, NOW);

		assertThat(response.paymentId()).isEqualTo(paymentId);
		assertThat(response.status()).isEqualTo("AUTHORIZED");
		assertThat(response.operationStatus()).isEqualTo("PENDING");

		assertThat(countRows("payment_operations", "payment_id = ? AND type = 'CAPTURE'", paymentId)).isEqualTo(1);
		assertThat(countRows("idempotency_keys", "payment_id = ? AND operation_scope = 'PAYMENT_CAPTURE'", paymentId))
				.isEqualTo(1);
		assertThat(countRows("payment_audit_events", "payment_id = ?", paymentId)).isEqualTo(1);
		assertThat(countRows("outbox_events", "payment_id = ?", paymentId)).isEqualTo(1);
	}

	@Test
	void equivalentReplayReturnsOriginalIdentityWithoutAdditionalWrites() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "replay-key");
		CreatePaymentResponse original = repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW);

		CreatePaymentResponse replay = repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW.plusSeconds(5));

		assertThat(replay).isEqualTo(original);
		assertThat(countRows("payment_operations", "payment_id = ? AND type = 'CAPTURE'", paymentId)).isEqualTo(1);
	}

	@Test
	void differingDerivedParametersOnSameKeyConflict() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "conflict-key");
		repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW);

		assertThatThrownBy(() -> repository.admitCapture(MERCHANT_ID, command, HASH_B, NOW))
				.isInstanceOf(IdempotencyKeyConflictException.class);
	}

	@Test
	void unknownPaymentIsRejected() {
		CaptureAdmissionCommand command = new CaptureAdmissionCommand(UUID.randomUUID(), 50_000L, "BRL", "key");

		assertThatThrownBy(() -> repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW))
				.isInstanceOf(PaymentNotFoundException.class);
	}

	@Test
	void alreadyCapturedPaymentWithNewKeyIsRejected() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CAPTURED", 50_000L, "BRL");

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "new-key");

		assertThatThrownBy(() -> repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW))
				.isInstanceOf(PaymentAlreadyCapturedException.class);
	}

	@Test
	void paymentInIneligibleStatusIsRejected() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CREATED", 50_000L, "BRL");

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "new-key");

		assertThatThrownBy(() -> repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW))
				.isInstanceOf(PaymentNotEligibleException.class);
	}

	@Test
	void existingUnresolvedCaptureOperationBlocksNewAdmission() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());
		seedOperation(UUID.randomUUID(), paymentId, "CAPTURE", "PROCESSING", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "another-key");

		assertThatThrownBy(() -> repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW))
				.isInstanceOf(PaymentOperationInProgressException.class);
	}

	@Test
	void precheckAdmissionReturnsEmptyForEligiblePayment() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "precheck-key");

		assertThat(repository.precheckAdmission(MERCHANT_ID, command, HASH_A)).isEmpty();
	}

	@Test
	void precheckAdmissionReturnsExistingAdmissionForEquivalentReplayWithoutInsertingAnything() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "precheck-replay-key");
		CreatePaymentResponse original = repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW);

		Optional<CreatePaymentResponse> precheck = repository.precheckAdmission(MERCHANT_ID, command, HASH_A);

		assertThat(precheck).contains(original);
		assertThat(countRows("payment_operations", "payment_id = ? AND type = 'CAPTURE'", paymentId)).isEqualTo(1);
	}

	@Test
	void precheckAdmissionReturnsExistingAdmissionEvenAfterOperationBecomesTerminal() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "precheck-terminal-key");
		CreatePaymentResponse original = repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW);

		jdbcClient.sql("UPDATE payment_operations SET status = 'SUCCEEDED' WHERE id = :operationId")
				.param("operationId", original.operationId())
				.update();
		jdbcClient.sql("UPDATE payments SET status = 'CAPTURED' WHERE id = :paymentId")
				.param("paymentId", paymentId)
				.update();

		Optional<CreatePaymentResponse> precheck = repository.precheckAdmission(MERCHANT_ID, command, HASH_A);

		assertThat(precheck).isPresent();
		assertThat(precheck.get().paymentId()).isEqualTo(original.paymentId());
		assertThat(precheck.get().operationId()).isEqualTo(original.operationId());
	}

	@Test
	void precheckAdmissionRejectsConflictingParametersOnSameKeyWithoutInsertingAnything() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "precheck-conflict-key");
		repository.admitCapture(MERCHANT_ID, command, HASH_A, NOW);

		assertThatThrownBy(() -> repository.precheckAdmission(MERCHANT_ID, command, HASH_B))
				.isInstanceOf(IdempotencyKeyConflictException.class);
	}

	@Test
	void precheckAdmissionRejectsUnknownPayment() {
		CaptureAdmissionCommand command = new CaptureAdmissionCommand(UUID.randomUUID(), 50_000L, "BRL", "key");

		assertThatThrownBy(() -> repository.precheckAdmission(MERCHANT_ID, command, HASH_A))
				.isInstanceOf(PaymentNotFoundException.class);
	}

	@Test
	void precheckAdmissionRejectsAlreadyCapturedPaymentWithNewKey() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CAPTURED", 50_000L, "BRL");

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "new-key");

		assertThatThrownBy(() -> repository.precheckAdmission(MERCHANT_ID, command, HASH_A))
				.isInstanceOf(PaymentAlreadyCapturedException.class);
	}

	@Test
	void precheckAdmissionRejectsPaymentInIneligibleStatus() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CREATED", 50_000L, "BRL");

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "new-key");

		assertThatThrownBy(() -> repository.precheckAdmission(MERCHANT_ID, command, HASH_A))
				.isInstanceOf(PaymentNotEligibleException.class);
	}

	@Test
	void precheckAdmissionRejectsExistingUnresolvedConflictingOperation() {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(UUID.randomUUID(), paymentId, "AUTHORIZE", "SUCCEEDED", UUID.randomUUID().toString());
		seedOperation(UUID.randomUUID(), paymentId, "CAPTURE", "PROCESSING", UUID.randomUUID().toString());

		CaptureAdmissionCommand command = new CaptureAdmissionCommand(paymentId, 50_000L, "BRL", "another-key");

		assertThatThrownBy(() -> repository.precheckAdmission(MERCHANT_ID, command, HASH_A))
				.isInstanceOf(PaymentOperationInProgressException.class);
	}

	private void seedPayment(UUID paymentId, String status, long amountMinor, String currency) {
		jdbcClient.sql("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (:id, :merchantId, :orderId, :amountMinor, :currency, :status, 0, :now, :now)
				""")
				.param("id", paymentId)
				.param("merchantId", MERCHANT_ID)
				.param("orderId", "order-" + paymentId)
				.param("amountMinor", amountMinor)
				.param("currency", currency)
				.param("status", status)
				.param("now", databaseTimestamp(NOW))
				.update();
	}

	private void seedOperation(UUID operationId, UUID paymentId, String type, String status, String pspIdempotencyKey) {
		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, psp_reference, attempt_count, created_at, updated_at)
				VALUES
				    (:id, :paymentId, :type, :status, 50000, :pspIdempotencyKey, :pspReference, 0, :now, :now)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("type", type)
				.param("status", status)
				.param("pspIdempotencyKey", pspIdempotencyKey)
				.param("pspReference", type.equals("AUTHORIZE") ? operationId.toString() : null)
				.param("now", databaseTimestamp(NOW))
				.update();
	}

	private int countRows(String table, String whereClause, UUID paymentId) {
		return jdbcClient.sql("SELECT COUNT(*) FROM " + table + " WHERE " + whereClause)
				.param(paymentId)
				.query(Integer.class)
				.single();
	}

	private OffsetDateTime databaseTimestamp(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}
}
