package jhsoftware.paymentsflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(properties = "spring.docker.compose.enabled=false")
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VoidAdmissionIntegrationTests {

	private static final UUID MERCHANT_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
	private static final OffsetDateTime BASE_TIME = OffsetDateTime.parse("2026-10-27T10:00:00Z");

	private static MockRestServiceServer pspValidityServer;

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@TestConfiguration(proxyBeanMethods = false)
	static class PspValidityTestConfiguration {

		@Bean
		@Primary
		RestClient testCaptureAuthorizationValidityRestClient() {
			RestClient.Builder builder = RestClient.builder();
			pspValidityServer = MockRestServiceServer.bindTo(builder).build();
			return builder.build();
		}
	}

	@Autowired
	MockMvc mockMvc;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@BeforeEach
	void cleanDatabase() {
		jdbcTemplate.update("DELETE FROM outbox_events");
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM idempotency_keys");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
		pspValidityServer.reset();
	}

	@AfterEach
	void verifyNoPspCallsWereEverMade() {
		pspValidityServer.verify();
	}

	@Test
	void unknownPaymentIdReturnsNotFoundWithoutPersistence() throws Exception {
		performVoid(UUID.randomUUID().toString(), "key").andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("PAYMENT_NOT_FOUND"));

		assertTableCountsZero();
	}

	@Test
	void malformedPaymentIdReturnsBadRequest() throws Exception {
		performVoid("not-a-uuid", "key")
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_ID"));
	}

	@Test
	void missingIdempotencyKeyReturnsBadRequestWithoutPersistence() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");

		mockMvc.perform(post("/payments/{paymentId}/void", paymentId))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_REQUEST"));

		assertTableCountsZero();
	}

	@Test
	void nonEmptyBodyReturnsBadRequestWithoutPersistence() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");

		mockMvc.perform(post("/payments/{paymentId}/void", paymentId)
					.header("Idempotency-Key", "key")
					.contentType(MediaType.APPLICATION_JSON)
					.content("{\"amount\":100}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_REQUEST"));

		assertTableCountsZero();
	}

	@Test
	void ineligiblePaymentStatusFailsWithoutPersistenceOrPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CREATED", 50_000L, "BRL");

		performVoid(paymentId.toString(), "key")
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("PAYMENT_NOT_ELIGIBLE"));

		assertTableCountsZero();
	}

	@Test
	void eligibleVoidIsAdmittedWithPendingOperationWithoutAnyPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID authorizeOperationId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(authorizeOperationId, paymentId, "AUTHORIZE", "SUCCEEDED", authorizeOperationId.toString());

		performVoid(paymentId.toString(), "void-key")
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.paymentId").value(paymentId.toString()))
				.andExpect(jsonPath("$.status").value("AUTHORIZED"))
				.andExpect(jsonPath("$.operationStatus").value("PENDING"));

		assertThat(jdbcTemplate.queryForObject(
						"SELECT count(*) FROM payment_operations WHERE payment_id = ? AND type = 'VOID'",
						Long.class,
						paymentId))
				.isEqualTo(1L);
		assertThat(jdbcTemplate.queryForObject(
						"SELECT count(*) FROM idempotency_keys WHERE payment_id = ? AND operation_scope = 'PAYMENT_VOID'",
						Long.class,
						paymentId))
				.isEqualTo(1L);
	}

	@Test
	void equivalentReplayReturnsOriginalIdentityWithoutAnyPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID authorizeOperationId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(authorizeOperationId, paymentId, "AUTHORIZE", "SUCCEEDED", authorizeOperationId.toString());

		String firstBody = performVoid(paymentId.toString(), "replay-key")
				.andExpect(status().isAccepted())
				.andReturn()
				.getResponse()
				.getContentAsString();

		performVoid(paymentId.toString(), "replay-key")
				.andExpect(status().isAccepted())
				.andExpect(content -> assertThat(content.getResponse().getContentAsString()).isEqualTo(firstBody));

		assertThat(jdbcTemplate.queryForObject(
						"SELECT count(*) FROM payment_operations WHERE payment_id = ? AND type = 'VOID'",
						Long.class,
						paymentId))
				.isEqualTo(1L);
	}

	@Test
	void replayAfterOperationBecomesTerminalStillReturnsOriginalIdentityWithoutAnyPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID authorizeOperationId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(authorizeOperationId, paymentId, "AUTHORIZE", "SUCCEEDED", authorizeOperationId.toString());

		performVoid(paymentId.toString(), "terminal-replay-key")
				.andExpect(status().isAccepted());

		UUID operationId = jdbcTemplate.queryForObject(
				"SELECT id FROM payment_operations WHERE payment_id = ? AND type = 'VOID'", UUID.class, paymentId);

		jdbcTemplate.update("UPDATE payment_operations SET status = 'SUCCEEDED' WHERE payment_id = ? AND type = 'VOID'", paymentId);
		jdbcTemplate.update("UPDATE payments SET status = 'CANCELED' WHERE id = ?", paymentId);

		performVoid(paymentId.toString(), "terminal-replay-key")
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.paymentId").value(paymentId.toString()))
				.andExpect(jsonPath("$.operationId").value(operationId.toString()))
				.andExpect(jsonPath("$.status").value("CANCELED"))
				.andExpect(jsonPath("$.operationStatus").value("SUCCEEDED"));
	}

	@Test
	void alreadyCapturedPaymentWithNewKeyReturnsConflictWithoutAnyPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "CAPTURED", 50_000L, "BRL");

		performVoid(paymentId.toString(), "new-key")
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("PAYMENT_ALREADY_CAPTURED"));

		assertTableCountsZero();
	}

	@Test
	void alreadyExpiredPaymentReturnsConflictWithoutAnyPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, "EXPIRED", 50_000L, "BRL");

		performVoid(paymentId.toString(), "new-key")
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("AUTHORIZATION_EXPIRED"));

		assertTableCountsZero();
	}

	@Test
	void existingUnresolvedCaptureOperationReturnsConflictWithoutAnyPspCall() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID authorizeOperationId = UUID.randomUUID();
		seedPayment(paymentId, "AUTHORIZED", 50_000L, "BRL");
		seedOperation(authorizeOperationId, paymentId, "AUTHORIZE", "SUCCEEDED", authorizeOperationId.toString());
		seedOperation(UUID.randomUUID(), paymentId, "CAPTURE", "PROCESSING", UUID.randomUUID().toString());

		performVoid(paymentId.toString(), "another-key")
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("PAYMENT_OPERATION_IN_PROGRESS"));
	}

	private org.springframework.test.web.servlet.ResultActions performVoid(String paymentId, String key) throws Exception {
		return mockMvc.perform(post("/payments/{paymentId}/void", paymentId)
				.header("Idempotency-Key", key));
	}

	private void assertTableCountsZero() {
		for (String table : java.util.List.of("idempotency_keys", "payment_audit_events", "outbox_events")) {
			assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM " + table, Long.class))
					.as("row count in %s", table)
					.isZero();
		}
		assertThat(jdbcTemplate.queryForObject(
						"SELECT count(*) FROM payment_operations WHERE type = 'VOID'", Long.class))
				.as("row count in payment_operations of type VOID")
				.isZero();
	}

	private void seedPayment(UUID paymentId, String status, long amountMinor, String currency) {
		jdbcTemplate.update("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (?, ?, ?, ?, ?, ?, 0, ?, ?)
				""",
				paymentId, MERCHANT_ID, "order-" + paymentId, amountMinor, currency, status, BASE_TIME, BASE_TIME);
	}

	private void seedOperation(UUID operationId, UUID paymentId, String type, String status, String pspReference) {
		jdbcTemplate.update("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, psp_reference, attempt_count, created_at, updated_at)
				VALUES
				    (?, ?, ?, ?, 50000, ?, ?, 0, ?, ?)
				""",
				operationId, paymentId, type, status, operationId.toString(), pspReference, BASE_TIME, BASE_TIME);
	}
}
