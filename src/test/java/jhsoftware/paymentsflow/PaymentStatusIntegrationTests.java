package jhsoftware.paymentsflow;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(properties = "spring.docker.compose.enabled=false")
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PaymentStatusIntegrationTests {

	private static final UUID MERCHANT_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
	private static final UUID OTHER_MERCHANT_ID = UUID.fromString("660e8400-e29b-41d4-a716-446655440000");
	private static final OffsetDateTime BASE_TIME = OffsetDateTime.parse("2026-10-25T10:00:00Z");

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	MockMvc mockMvc;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@BeforeEach
	void cleanDatabase() {
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM idempotency_keys");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
	}

	@Test
	void knownPaymentWithFinalizedOperationReturnsSeparateStatusesAndLatestOperation() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPayment(paymentId, MERCHANT_ID, "order-1", 50_000L, "BRL", "AUTHORIZED");
		seedOperation(operationId, paymentId, "AUTHORIZE", "SUCCEEDED", BASE_TIME);

		mockMvc.perform(get("/payments/{paymentId}", paymentId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.paymentId").value(paymentId.toString()))
				.andExpect(jsonPath("$.orderId").value("order-1"))
				.andExpect(jsonPath("$.amount").value(50_000))
				.andExpect(jsonPath("$.currency").value("BRL"))
				.andExpect(jsonPath("$.status").value("AUTHORIZED"))
				.andExpect(jsonPath("$.latestOperation.operationId").value(operationId.toString()))
				.andExpect(jsonPath("$.latestOperation.type").value("AUTHORIZE"))
				.andExpect(jsonPath("$.latestOperation.status").value("SUCCEEDED"));
	}

	@Test
	void knownPaymentWithNoOperationYetReturnsNullLatestOperation() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, MERCHANT_ID, "order-2", 10_000L, "USD", "CREATED");

		mockMvc.perform(get("/payments/{paymentId}", paymentId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("CREATED"))
				.andExpect(jsonPath("$.latestOperation").value(Matchers.nullValue()));
	}

	@Test
	void knownPaymentWithUnresolvedLatestOperationKeepsPaymentAndOperationStatusesDistinct() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID firstOperationId = UUID.randomUUID();
		UUID latestOperationId = UUID.randomUUID();
		seedPayment(paymentId, MERCHANT_ID, "order-3", 20_000L, "BRL", "CREATED");
		seedOperation(firstOperationId, paymentId, "AUTHORIZE", "FAILED", BASE_TIME);
		seedOperation(latestOperationId, paymentId, "AUTHORIZE", "UNKNOWN", BASE_TIME.plusSeconds(5));

		mockMvc.perform(get("/payments/{paymentId}", paymentId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("CREATED"))
				.andExpect(jsonPath("$.latestOperation.operationId").value(latestOperationId.toString()))
				.andExpect(jsonPath("$.latestOperation.status").value("UNKNOWN"));
	}

	@Test
	void latestOperationTieBreaksDeterministicallyByIdWhenCreatedAtMatches() throws Exception {
		UUID paymentId = UUID.randomUUID();
		UUID lowerOperationId = UUID.fromString("00000000-0000-0000-0000-000000000001");
		UUID higherOperationId = UUID.fromString("00000000-0000-0000-0000-000000000002");
		seedPayment(paymentId, MERCHANT_ID, "order-4", 30_000L, "BRL", "CREATED");
		seedOperation(lowerOperationId, paymentId, "AUTHORIZE", "FAILED", BASE_TIME);
		seedOperation(higherOperationId, paymentId, "AUTHORIZE", "UNKNOWN", BASE_TIME);

		mockMvc.perform(get("/payments/{paymentId}", paymentId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.latestOperation.operationId").value(higherOperationId.toString()));
	}

	@Test
	void unknownPaymentIdReturnsNotFound() throws Exception {
		mockMvc.perform(get("/payments/{paymentId}", UUID.randomUUID()))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("PAYMENT_NOT_FOUND"));
	}

	@Test
	void malformedPaymentIdReturnsBadRequest() throws Exception {
		mockMvc.perform(get("/payments/{paymentId}", "not-a-uuid"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_ID"));
	}

	@Test
	void paymentBelongingToAnotherMerchantReturnsNotFound() throws Exception {
		UUID paymentId = UUID.randomUUID();
		seedPayment(paymentId, OTHER_MERCHANT_ID, "order-5", 40_000L, "BRL", "CREATED");

		mockMvc.perform(get("/payments/{paymentId}", paymentId))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("PAYMENT_NOT_FOUND"));
	}

	private void seedPayment(
			UUID paymentId, UUID merchantId, String orderId, long amountMinor, String currency, String status) {
		jdbcTemplate.update("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, version, created_at, updated_at)
				VALUES
				    (?, ?, ?, ?, ?, ?, 0, ?, ?)
				""",
				paymentId, merchantId, orderId, amountMinor, currency, status,
				BASE_TIME, BASE_TIME);
	}

	private void seedOperation(
			UUID operationId, UUID paymentId, String type, String status, OffsetDateTime createdAt) {
		jdbcTemplate.update("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count, created_at, updated_at)
				VALUES
				    (?, ?, ?, ?, 10000, ?, 0, ?, ?)
				""",
				operationId, paymentId, type, status, operationId.toString(), createdAt, createdAt);
	}
}
