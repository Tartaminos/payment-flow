package jhsoftware.pspsimulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(properties = "spring.docker.compose.enabled=false")
@AutoConfigureMockMvc
class PspOperationIntegrationTests {

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	MockMvc mockMvc;

	@Autowired
	JdbcTemplate jdbcTemplate;

	private String request(UUID pspOperationId, long amountMinor) {
		return request(pspOperationId, UUID.randomUUID(), amountMinor);
	}

	private String request(UUID pspOperationId, UUID merchantId, long amountMinor) {
		return """
				{"pspOperationId":"%s","merchantId":"%s","paymentId":"%s","amountMinor":%d,"currency":"BRL"}
				"""
				.formatted(pspOperationId, merchantId, UUID.randomUUID(), amountMinor);
	}

	@Test
	void approvedAmountYieldsSucceededAndIsIdempotentOnReplay() throws Exception {
		UUID pspOperationId = UUID.randomUUID();
		String body = request(pspOperationId, 10000L);

		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));

		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));

		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(1L);
	}

	@Test
	void mismatchedReplayOfSameIdentityIsRejectedAsConflict() throws Exception {
		UUID pspOperationId = UUID.randomUUID();
		UUID merchantId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(pspOperationId, merchantId, 10000L)))
				.andExpect(status().isOk());

		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(pspOperationId, merchantId, 20000L)))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("PSP_OPERATION_CONFLICT"));
	}

	@Test
	void declinedAmountBandYieldsRejected() throws Exception {
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(UUID.randomUUID(), 1750L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("REJECTED"));
	}

	@Test
	void nonExecutedAmountBandYieldsFailed() throws Exception {
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(UUID.randomUUID(), 1820L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("FAILED"));
	}

	@Test
	void expiredAuthorizationValidityAmountBandYieldsSucceededDecisionWithExpiredValidity() throws Exception {
		UUID pspOperationId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(pspOperationId, 1680L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"))
				.andExpect(jsonPath("$.validity").value("EXPIRED"));

		mockMvc.perform(get("/simulator/operations/{id}", pspOperationId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.validity").value("EXPIRED"));
	}

	@Test
	void processingAmountBandYieldsAccepted() throws Exception {
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(UUID.randomUUID(), 1870L)))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.result").value("PROCESSING"));
	}

	@Test
	void unprocessedFaultAmountBandYieldsNoPersistedEffect() throws Exception {
		UUID pspOperationId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(pspOperationId, 1960L)))
				.andExpect(status().isServiceUnavailable());

		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(0L);
	}

	@Test
	void committedReplyDroppedFaultPersistsSucceededButReportsServiceUnavailable() throws Exception {
		UUID pspOperationId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(pspOperationId, 1910L)))
				.andExpect(status().isServiceUnavailable());

		mockMvc.perform(get("/simulator/operations/{id}", pspOperationId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));
	}

	@Test
	void concurrentRequestsForSameIdentityPersistExactlyOneDecisionAndNeverConflict() throws Exception {
		UUID pspOperationId = UUID.randomUUID();
		String body = request(pspOperationId, 10000L);
		int concurrency = 8;
		CyclicBarrier barrier = new CyclicBarrier(concurrency);
		ExecutorService executor = Executors.newFixedThreadPool(concurrency);

		List<Future<Integer>> futures = new ArrayList<>();
		for (int i = 0; i < concurrency; i++) {
			futures.add(executor.submit(() -> {
				barrier.await();
				return mockMvc.perform(post("/simulator/operations/authorize")
						.contentType(MediaType.APPLICATION_JSON)
						.content(body))
						.andReturn()
						.getResponse()
						.getStatus();
			}));
		}

		List<Integer> statuses = new ArrayList<>();
		for (Future<Integer> future : futures) {
			statuses.add(future.get(10, TimeUnit.SECONDS));
		}
		executor.shutdown();

		assertThat(statuses).allSatisfy(status -> assertThat(status).isEqualTo(200));
		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(1L);
	}

	@Test
	void lookupOfUnknownPspOperationIdReturnsNotFound() throws Exception {
		mockMvc.perform(get("/simulator/operations/{id}", UUID.randomUUID()))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("PSP_OPERATION_NOT_FOUND"));
	}

	private String captureRequest(UUID pspOperationId, UUID merchantId, UUID authorizationPspOperationId, long amountMinor) {
		return """
				{"pspOperationId":"%s","merchantId":"%s","paymentId":"%s","amountMinor":%d,"currency":"BRL","authorizationPspOperationId":"%s"}
				"""
				.formatted(pspOperationId, merchantId, UUID.randomUUID(), amountMinor, authorizationPspOperationId);
	}

	private String captureRequest(UUID pspOperationId, UUID authorizationPspOperationId, long amountMinor) {
		return captureRequest(pspOperationId, UUID.randomUUID(), authorizationPspOperationId, amountMinor);
	}

	private UUID authorizeApprovedAuthorization(long amountMinor) throws Exception {
		UUID authorizationPspOperationId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(authorizationPspOperationId, amountMinor)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"))
				.andExpect(jsonPath("$.validity").value("ACTIVE"));
		return authorizationPspOperationId;
	}

	private UUID authorizeExpiredAuthorization(long amountMinor) throws Exception {
		UUID authorizationPspOperationId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/authorize")
				.contentType(MediaType.APPLICATION_JSON)
				.content(request(authorizationPspOperationId, amountMinor)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"))
				.andExpect(jsonPath("$.validity").value("EXPIRED"));
		return authorizationPspOperationId;
	}

	@Test
	void capturedAmountBandYieldsSucceededAndIsIdempotentOnReplay() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(10000L);
		UUID pspOperationId = UUID.randomUUID();
		String body = captureRequest(pspOperationId, authorizationPspOperationId, 10000L);

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));

		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(1L);
	}

	@Test
	void mismatchedCaptureReplayOfSameIdentityIsRejectedAsConflict() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(10000L);
		UUID pspOperationId = UUID.randomUUID();
		UUID merchantId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(captureRequest(pspOperationId, merchantId, authorizationPspOperationId, 10000L)))
				.andExpect(status().isOk());

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(captureRequest(pspOperationId, merchantId, UUID.randomUUID(), 10000L)))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("PSP_OPERATION_CONFLICT"));
	}

	@Test
	void expiredAuthorizationForcesCaptureRejectionEvenUnderAnApprovedCaptureFixtureBand() throws Exception {
		UUID authorizationPspOperationId = authorizeExpiredAuthorization(1680L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(captureRequest(pspOperationId, authorizationPspOperationId, 1680L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("REJECTED"))
				.andExpect(jsonPath("$.reasonCode").value("AUTHORIZATION_EXPIRED"));
	}

	@Test
	void captureDeclinedFixtureBandYieldsRejectedIndependentlyOfTheAuthorizeBand() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(150L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(captureRequest(pspOperationId, authorizationPspOperationId, 150L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("REJECTED"));
	}

	@Test
	void captureNonExecutedFixtureBandYieldsFailedIndependentlyOfTheAuthorizeBand() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(250L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(captureRequest(pspOperationId, authorizationPspOperationId, 250L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("FAILED"));
	}

	@Test
	void captureUnprocessedFaultFixtureBandYieldsNoPersistedEffect() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(520L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/capture")
				.contentType(MediaType.APPLICATION_JSON)
				.content(captureRequest(pspOperationId, authorizationPspOperationId, 520L)))
				.andExpect(status().isServiceUnavailable());

		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(0L);
	}

	private String voidRequest(UUID pspOperationId, UUID merchantId, UUID authorizationPspOperationId, long amountMinor) {
		return """
				{"pspOperationId":"%s","merchantId":"%s","paymentId":"%s","amountMinor":%d,"currency":"BRL","authorizationPspOperationId":"%s"}
				"""
				.formatted(pspOperationId, merchantId, UUID.randomUUID(), amountMinor, authorizationPspOperationId);
	}

	private String voidRequest(UUID pspOperationId, UUID authorizationPspOperationId, long amountMinor) {
		return voidRequest(pspOperationId, UUID.randomUUID(), authorizationPspOperationId, amountMinor);
	}

	@Test
	void voidedAmountBandYieldsSucceededAndIsIdempotentOnReplay() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(10000L);
		UUID pspOperationId = UUID.randomUUID();
		String body = voidRequest(pspOperationId, authorizationPspOperationId, 10000L);

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("SUCCEEDED"));

		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(1L);
	}

	@Test
	void mismatchedVoidReplayOfSameIdentityIsRejectedAsConflict() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(10000L);
		UUID pspOperationId = UUID.randomUUID();
		UUID merchantId = UUID.randomUUID();
		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, merchantId, authorizationPspOperationId, 10000L)))
				.andExpect(status().isOk());

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, merchantId, UUID.randomUUID(), 10000L)))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("PSP_OPERATION_CONFLICT"));
	}

	@Test
	void expiredAuthorizationForcesVoidRejectionEvenUnderAnApprovedVoidFixtureBand() throws Exception {
		UUID authorizationPspOperationId = authorizeExpiredAuthorization(1680L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, authorizationPspOperationId, 1680L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("REJECTED"))
				.andExpect(jsonPath("$.reasonCode").value("AUTHORIZATION_EXPIRED"));
	}

	@Test
	void voidDeclinedFixtureBandYieldsRejectedIndependentlyOfTheAuthorizeAndCaptureBands() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(310L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, authorizationPspOperationId, 310L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("REJECTED"));
	}

	@Test
	void voidNonExecutedFixtureBandYieldsFailedIndependentlyOfTheAuthorizeAndCaptureBands() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(360L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, authorizationPspOperationId, 360L)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.result").value("FAILED"));
	}

	@Test
	void voidProcessingFixtureBandYieldsAccepted() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(410L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, authorizationPspOperationId, 410L)))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.result").value("PROCESSING"));
	}

	@Test
	void voidUnprocessedFaultFixtureBandYieldsNoPersistedEffect() throws Exception {
		UUID authorizationPspOperationId = authorizeApprovedAuthorization(490L);
		UUID pspOperationId = UUID.randomUUID();

		mockMvc.perform(post("/simulator/operations/void")
				.contentType(MediaType.APPLICATION_JSON)
				.content(voidRequest(pspOperationId, authorizationPspOperationId, 490L)))
				.andExpect(status().isServiceUnavailable());

		Long count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM psp_operations WHERE psp_operation_id = ?", Long.class, pspOperationId);
		assertThat(count).isEqualTo(0L);
	}
}
