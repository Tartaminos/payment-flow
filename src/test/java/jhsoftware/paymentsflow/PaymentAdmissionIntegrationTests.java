package jhsoftware.paymentsflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(properties = "spring.docker.compose.enabled=false")
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PaymentAdmissionIntegrationTests {

	private static final UUID MERCHANT_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
	private static final String VALID_BODY = """
			{"orderId":"150","amount":50000,"currency":"BRL"}
			""";

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	MockMvc mockMvc;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@BeforeEach
	void cleanDatabase() {
		dropOutboxFailureTrigger();
		jdbcTemplate.update("DELETE FROM outbox_events");
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM idempotency_keys");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
	}

	@AfterAll
	void removeTestFixture() {
		dropOutboxFailureTrigger();
	}

	@Test
	void validRequestReturnsAcceptedAndCommitsTheFiveLinkedRecords() throws Exception {
		MvcResult result = performPayment("checkout-150-attempt-1", VALID_BODY)
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.paymentId").isNotEmpty())
				.andExpect(jsonPath("$.operationId").isNotEmpty())
				.andExpect(jsonPath("$.status").value("CREATED"))
				.andExpect(jsonPath("$.operationStatus").value("PENDING"))
				.andReturn();

		UUID paymentId = UUID.fromString(JsonPath.read(result.getResponse().getContentAsString(), "$.paymentId"));
		UUID operationId = UUID.fromString(JsonPath.read(result.getResponse().getContentAsString(), "$.operationId"));

		assertTableCounts(1);

		Map<String, Object> payment = jdbcTemplate.queryForMap("SELECT * FROM payments WHERE id = ?", paymentId);
		assertThat(payment)
				.containsEntry("merchant_id", MERCHANT_ID)
				.containsEntry("external_order_id", "150")
				.containsEntry("amount_minor", 50_000L)
				.containsEntry("currency", "BRL")
				.containsEntry("status", "CREATED")
				.containsEntry("version", 0L);

		Map<String, Object> operation = jdbcTemplate.queryForMap(
				"SELECT * FROM payment_operations WHERE id = ?", operationId);
		assertThat(operation)
				.containsEntry("payment_id", paymentId)
				.containsEntry("type", "AUTHORIZE")
				.containsEntry("status", "PENDING")
				.containsEntry("amount_minor", 50_000L)
				.containsEntry("psp_idempotency_key", operationId.toString())
				.containsEntry("attempt_count", 0);

		Map<String, Object> idempotency = jdbcTemplate.queryForMap("SELECT * FROM idempotency_keys");
		assertThat(idempotency)
				.containsEntry("merchant_id", MERCHANT_ID)
				.containsEntry("operation_scope", "PAYMENT_CREATE")
				.containsEntry("key", "checkout-150-attempt-1")
				.containsEntry("request_hash", "bb1e827652151caf3837ee08fb9e6ab5876dddd938892bed8e28e6ad05ae1d6e")
				.containsEntry("payment_id", paymentId)
				.containsEntry("payment_operation_id", operationId)
				.containsEntry("expires_at", null);

		Map<String, Object> audit = jdbcTemplate.queryForMap("SELECT * FROM payment_audit_events");
		assertThat(audit)
				.containsEntry("payment_id", paymentId)
				.containsEntry("payment_operation_id", operationId)
				.containsEntry("entity_type", "PAYMENT")
				.containsEntry("event_type", "PAYMENT_CREATED")
				.containsEntry("new_status", "CREATED")
				.containsEntry("source", "API")
				.containsEntry("sequence_number", 1L);
		assertThat(audit.get("previous_status")).isNull();
		assertThat(audit.get("causation_id")).isNull();

		Map<String, Object> outbox = jdbcTemplate.queryForMap("SELECT * FROM outbox_events");
		assertThat(outbox)
				.containsEntry("payment_id", paymentId)
				.containsEntry("operation_id", operationId)
				.containsEntry("command_type", "ExecutePaymentOperation")
				.containsEntry("status", "PENDING")
				.containsEntry("correlation_id", audit.get("correlation_id"));
		assertThat(outbox.get("causation_id")).isNull();
		assertThat(outbox.get("published_at")).isNull();
	}

	@Test
	void printableAsciiIdempotencyKeyWithSpacesIsAcceptedAndPersistedWithoutNormalization() throws Exception {
		String key = " checkout  attempt 1 ";

		performPayment(key, VALID_BODY)
				.andExpect(status().isAccepted());

		assertThat(jdbcTemplate.queryForObject("SELECT key FROM idempotency_keys", String.class))
				.isEqualTo(key);
	}

	@ParameterizedTest(name = "rejects invalid body: {0}")
	@MethodSource("invalidBodies")
	void invalidBodiesReturnSafeBadRequestWithoutPersistence(String description, String body) throws Exception {
		performPayment("valid-key", body)
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_REQUEST"))
				.andExpect(jsonPath("$.message").value("The payment request is invalid."));

		assertTableCounts(0);
	}

	static Stream<Arguments> invalidBodies() {
		return Stream.of(
				Arguments.of("unknown field", "{\"orderId\":\"150\",\"amount\":50000,\"currency\":\"BRL\",\"card\":\"x\"}"),
				Arguments.of("missing field", "{\"orderId\":\"150\",\"amount\":50000}"),
				Arguments.of("edge whitespace in order", "{\"orderId\":\" 150\",\"amount\":50000,\"currency\":\"BRL\"}"),
				Arguments.of("zero amount", "{\"orderId\":\"150\",\"amount\":0,\"currency\":\"BRL\"}"),
				Arguments.of("decimal amount", "{\"orderId\":\"150\",\"amount\":50000.0,\"currency\":\"BRL\"}"),
				Arguments.of("string amount", "{\"orderId\":\"150\",\"amount\":\"50000\",\"currency\":\"BRL\"}"),
				Arguments.of("amount beyond bigint", "{\"orderId\":\"150\",\"amount\":9223372036854775808,\"currency\":\"BRL\"}"),
				Arguments.of("lowercase currency", "{\"orderId\":\"150\",\"amount\":50000,\"currency\":\"brl\"}"),
				Arguments.of("malformed json", "{\"orderId\":\"150\""));
	}

	@ParameterizedTest(name = "rejects invalid idempotency key: {0}")
	@MethodSource("invalidKeys")
	void invalidIdempotencyKeysReturnSafeBadRequestWithoutPersistence(String description, String key) throws Exception {
		performPayment(key, VALID_BODY)
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_REQUEST"));

		assertTableCounts(0);
	}

	static Stream<Arguments> invalidKeys() {
		return Stream.of(
				Arguments.of("empty", ""),
				Arguments.of("blank", " "),
				Arguments.of("more than 150 chars", "k".repeat(151)),
				Arguments.of("non ASCII", "pagamento-á"));
	}

	@Test
	void missingIdempotencyKeyReturnsSafeBadRequestWithoutPersistence() throws Exception {
		mockMvc.perform(post("/payments")
					.contentType(MediaType.APPLICATION_JSON)
					.content(VALID_BODY))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_PAYMENT_REQUEST"));

		assertTableCounts(0);
	}

	@Test
	void equivalentReplayReturnsOriginalIdentityAndCurrentStatusesWithoutAdditionalWrites() throws Exception {
		MvcResult original = performPayment("replay-key", VALID_BODY)
				.andExpect(status().isAccepted())
				.andReturn();
		String originalPaymentId = JsonPath.read(original.getResponse().getContentAsString(), "$.paymentId");
		String originalOperationId = JsonPath.read(original.getResponse().getContentAsString(), "$.operationId");

		jdbcTemplate.update("UPDATE payments SET status = 'AUTHORIZED' WHERE id = ?", UUID.fromString(originalPaymentId));
		jdbcTemplate.update(
				"UPDATE payment_operations SET status = 'PROCESSING' WHERE id = ?",
				UUID.fromString(originalOperationId));

		performPayment("replay-key", """
				{"currency":"BRL", "amount":50000, "orderId":"150"}
				""")
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.paymentId").value(originalPaymentId))
				.andExpect(jsonPath("$.operationId").value(originalOperationId))
				.andExpect(jsonPath("$.status").value("AUTHORIZED"))
				.andExpect(jsonPath("$.operationStatus").value("PROCESSING"));

		assertTableCounts(1);
	}

	@ParameterizedTest(name = "rejects same idempotency key with different {0}")
	@MethodSource("conflictingBodies")
	void conflictingReplayReturnsExactConflictAndPreservesOriginal(String field, String conflictingBody) throws Exception {
		performPayment("conflict-key", VALID_BODY)
				.andExpect(status().isAccepted());

		performPayment("conflict-key", conflictingBody)
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_CONFLICT"))
				.andExpect(jsonPath("$.message").value(
						"The idempotency key is already used for different payment details."));

		assertTableCounts(1);
		assertThat(jdbcTemplate.queryForMap("SELECT external_order_id, amount_minor, currency FROM payments"))
				.containsEntry("external_order_id", "150")
				.containsEntry("amount_minor", 50_000L)
				.containsEntry("currency", "BRL");
	}

	static Stream<Arguments> conflictingBodies() {
		return Stream.of(
				Arguments.of("order ID", "{\"orderId\":\"151\",\"amount\":50000,\"currency\":\"BRL\"}"),
				Arguments.of("amount", "{\"orderId\":\"150\",\"amount\":50001,\"currency\":\"BRL\"}"),
				Arguments.of("currency", "{\"orderId\":\"150\",\"amount\":50000,\"currency\":\"USD\"}"));
	}

	@Test
	void concurrentEquivalentRequestsBothResolveToOneCompleteGraph() throws Exception {
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);
		List<Future<MvcResult>> responses = new ArrayList<>();

		try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
			for (int index = 0; index < 2; index++) {
				responses.add(executor.submit(() -> {
					ready.countDown();
					start.await();
					return performPayment("competing-key", VALID_BODY)
							.andReturn();
				}));
			}

			ready.await();
			start.countDown();
			List<MvcResult> results = responses.stream().map(this::get).toList();
			assertThat(results).extracting(result -> result.getResponse().getStatus())
					.containsExactly(202, 202);
			assertThat(results).extracting(this::responseIdentity).containsOnly(responseIdentity(results.getFirst()));
		}

		assertTableCounts(1);
	}

	@Test
	void concurrentDifferentRequestsProduceOneWinnerAndOneConflict() throws Exception {
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);
		List<String> bodies = List.of(
				VALID_BODY,
				"{\"orderId\":\"other-order\",\"amount\":70000,\"currency\":\"USD\"}");
		List<Future<MvcResult>> responses = new ArrayList<>();

		try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
			for (String body : bodies) {
				responses.add(executor.submit(() -> {
					ready.countDown();
					start.await();
					return performPayment("different-competing-key", body).andReturn();
				}));
			}

			ready.await();
			start.countDown();
			List<MvcResult> results = responses.stream().map(this::get).toList();
			assertThat(results).extracting(result -> result.getResponse().getStatus())
					.containsExactlyInAnyOrder(202, 409);
			MvcResult conflict = results.stream()
					.filter(result -> result.getResponse().getStatus() == 409)
					.findFirst()
					.orElseThrow();
			assertThat(JsonPath.<String>read(conflict.getResponse().getContentAsString(), "$.code"))
					.isEqualTo("IDEMPOTENCY_KEY_CONFLICT");
			MvcResult accepted = results.stream()
					.filter(result -> result.getResponse().getStatus() == 202)
					.findFirst()
					.orElseThrow();
			assertThat(jdbcTemplate.queryForObject("SELECT id::text FROM payments", String.class))
					.isEqualTo(JsonPath.<String>read(accepted.getResponse().getContentAsString(), "$.paymentId"));
		}

		assertTableCounts(1);
	}

	@Test
	void replayFromFreshApplicationContextUsesPersistedAdmission() throws Exception {
		MvcResult original = performPayment("restart-key", VALID_BODY)
				.andExpect(status().isAccepted())
				.andReturn();
		String originalIdentity = responseIdentity(original);

		try (ConfigurableApplicationContext restarted = new SpringApplicationBuilder(PaymentsflowApplication.class)
				.web(WebApplicationType.SERVLET)
				.properties(
						"server.port=0",
						"spring.docker.compose.enabled=false",
						"spring.datasource.url=" + postgres.getJdbcUrl(),
						"spring.datasource.username=" + postgres.getUsername(),
						"spring.datasource.password=" + postgres.getPassword())
				.run()) {
			int port = restarted.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
			HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/payments"))
					.header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
					.header("Idempotency-Key", "restart-key")
					.POST(HttpRequest.BodyPublishers.ofString(VALID_BODY))
					.build();

			HttpResponse<String> replay = HttpClient.newHttpClient()
					.send(request, HttpResponse.BodyHandlers.ofString());

			assertThat(replay.statusCode()).isEqualTo(202);
			assertThat(responseIdentity(replay.body())).isEqualTo(originalIdentity);
		}

		assertTableCounts(1);
	}

	@Test
	void failureOnFinalOutboxInsertRollsBackEveryEarlierInsert() throws Exception {
		installOutboxFailureTrigger();
		try {
			performPayment("rollback-key", VALID_BODY)
					.andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.code").value("INTERNAL_SERVER_ERROR"))
					.andExpect(jsonPath("$.message").value("The payment request could not be processed."));
		}
		finally {
			dropOutboxFailureTrigger();
		}

		assertTableCounts(0);
	}

	private org.springframework.test.web.servlet.ResultActions performPayment(String key, String body) throws Exception {
		return mockMvc.perform(post("/payments")
				.header("Idempotency-Key", key)
				.contentType(MediaType.APPLICATION_JSON)
				.content(body));
	}

	private void assertTableCounts(int expected) {
		for (String table : List.of(
				"payments", "payment_operations", "idempotency_keys", "payment_audit_events", "outbox_events")) {
			assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM " + table, Long.class))
					.as("row count in %s", table)
					.isEqualTo(expected);
		}
	}

	private <T> T get(Future<T> future) {
		try {
			return future.get();
		}
		catch (Exception exception) {
			throw new AssertionError("Concurrent request did not complete", exception);
		}
	}

	private String responseIdentity(MvcResult result) {
		return responseIdentity(new String(result.getResponse().getContentAsByteArray(), StandardCharsets.UTF_8));
	}

	private String responseIdentity(String body) {
		return JsonPath.<String>read(body, "$.paymentId") + ":" + JsonPath.<String>read(body, "$.operationId");
	}

	private void installOutboxFailureTrigger() {
		jdbcTemplate.execute("""
				CREATE FUNCTION fail_test_outbox_insert() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
				    RAISE EXCEPTION 'forced test failure after earlier admission inserts';
				END;
				$$
				""");
		jdbcTemplate.execute("""
				CREATE TRIGGER test_outbox_insert_failure
				BEFORE INSERT ON outbox_events
				FOR EACH ROW EXECUTE FUNCTION fail_test_outbox_insert()
				""");
	}

	private void dropOutboxFailureTrigger() {
		jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_outbox_insert_failure ON outbox_events");
		jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_test_outbox_insert()");
	}
}
