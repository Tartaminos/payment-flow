package jhsoftware.paymentsflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.jayway.jsonpath.JsonPath;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@EnabledIfSystemProperty(named = "paymentflow.real-messaging-test", matches = "true")
@Testcontainers
@SpringBootTest(properties = {
		"spring.docker.compose.enabled=false",
		"spring.kafka.bootstrap-servers=localhost:9092",
		"spring.kafka.producer.properties.schema.registry.url=http://localhost:8082",
		"paymentflow.messaging.enabled=true",
		"paymentflow.messaging.schema-registry-url=http://localhost:8082",
		"paymentflow.messaging.publisher.poll-interval=200ms",
		"paymentflow.processor.psp-base-url=http://localhost:8081",
		"paymentflow.processor.retry-gap-after-first-attempt=1ms",
		"paymentflow.processor.retry-gap-after-second-attempt=1ms"
})
@AutoConfigureMockMvc
@TestMethodOrder(OrderAnnotation.class)
class M6EndToEndFailureRecoveryIntegrationTests {

	private static final String MAIN_TOPIC = "payment.operations.requested.v1";
	private static final String MAIN_LISTENER = "payment-operation-command-listener";

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	MockMvc mockMvc;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@Autowired
	Schema executePaymentOperationSchema;

	@Autowired
	KafkaListenerEndpointRegistry listenerRegistry;

	@Autowired
	MeterRegistry meterRegistry;

	private final RestClient psp = RestClient.create("http://localhost:8081");

	@BeforeEach
	void cleanApplicationDatabase() {
		jdbcTemplate.execute("""
				TRUNCATE TABLE reconciliation_escalations, reconciliation_jobs, quarantined_commands,
				               scheduled_retries, outbox_events, payment_audit_events, idempotency_keys,
				               payment_operations, payments CASCADE
				""");
	}

	@Test
	@Order(1)
	void droppedClientResponseReplayRecoversOneAuthorizationAcrossTheCompleteWorkflow() throws Exception {
		String key = "m6-client-drop-" + UUID.randomUUID();
		String body = requestBody("m6-client-drop", 10_000L);

		MvcResult droppedResponse = admit(key, body);
		UUID paymentId = paymentId(droppedResponse);
		UUID operationId = operationId(droppedResponse);
		MvcResult replay = admit(key, body);

		assertThat(paymentId(replay)).isEqualTo(paymentId);
		assertThat(operationId(replay)).isEqualTo(operationId);
		await("authorization to become durable", () -> "SUCCEEDED".equals(operationStatus(operationId)));

		assertThat(paymentStatus(paymentId)).isEqualTo("AUTHORIZED");
		assertThat(count("payment_operations", "payment_id", paymentId)).isEqualTo(1L);
		assertThat(count("outbox_events", "operation_id", operationId)).isEqualTo(1L);
		assertThat(outboxStatus(operationId)).isEqualTo("PUBLISHED");
		assertThat(pspDecision(operationId)).containsEntry("result", "SUCCEEDED");
		assertThat(auditEvents(paymentId)).contains("PAYMENT_CREATED", "PAYMENT_OPERATION_SUCCEEDED", "PAYMENT_AUTHORIZED");
		assertThat(auditJson(paymentId).toLowerCase())
				.doesNotContain("\"pan\"", "\"cvv\"", "\"secret\"", "\"token\"", "rawproviderpayload");
	}

	@Test
	@Order(2)
	void persistedPspEffectWithDroppedReplyReusesTheOriginalFinancialIdentity() throws Exception {
		MvcResult response = admit(
				"m6-psp-reply-drop-" + UUID.randomUUID(), requestBody("m6-psp-reply-drop", 1_910L));
		UUID paymentId = paymentId(response);
		UUID operationId = operationId(response);

		await("the committed PSP decision to be recovered", () -> "SUCCEEDED".equals(operationStatus(operationId)));

		assertThat(paymentStatus(paymentId)).isEqualTo("AUTHORIZED");
		assertThat(attemptCount(operationId)).isBetween(1, 3);
		assertThat(pspDecision(operationId))
				.containsEntry("pspOperationId", operationId.toString())
				.containsEntry("result", "SUCCEEDED");
		assertThat(count("payment_operations", "id", operationId)).isEqualTo(1L);
		assertThat(auditEventCount(paymentId, "PAYMENT_OPERATION_SUCCEEDED")).isEqualTo(1L);
	}

	@Test
	@Order(6)
	void unprocessedPspFailureRemainsVisibleAndRecoverableWithoutFabricatedFailure() throws Exception {
		MvcResult response = admit(
				"m6-visible-unknown-" + UUID.randomUUID(), requestBody("m6-visible-unknown", 1_960L));
		UUID paymentId = paymentId(response);
		UUID operationId = operationId(response);

		await("ambiguous PSP attempts to become UNKNOWN", () -> "UNKNOWN".equals(operationStatus(operationId)));

		assertThat(paymentStatus(paymentId)).isEqualTo("CREATED");
		assertThat(attemptCount(operationId)).isEqualTo(3);
		assertThat(count("reconciliation_jobs", "operation_id", operationId)).isEqualTo(1L);
		assertThat(operationStatus(operationId)).isNotIn("FAILED", "REJECTED");
		assertThat(auditEventCount(paymentId, "PAYMENT_OPERATION_UNKNOWN")).isEqualTo(1L);
	}

	@Test
	@Order(3)
	void captureVoidAndExpiryUseTheRealKafkaAndStatefulPspBoundaries() throws Exception {
		PaymentIdentity capturePayment = authorizePayment("m6-capture", 10_000L);
		MvcResult capture = mockMvc.perform(post("/payments/{paymentId}/capture", capturePayment.paymentId())
				.header("Idempotency-Key", "m6-capture-" + UUID.randomUUID()))
				.andExpect(status().isAccepted())
				.andReturn();
		UUID captureOperationId = operationId(capture);
		await("capture to complete through Kafka and PSP", () -> "CAPTURED".equals(paymentStatus(capturePayment.paymentId())));
		assertThat(operationStatus(captureOperationId)).isEqualTo("SUCCEEDED");
		assertThat(pspDecision(captureOperationId)).containsEntry("result", "SUCCEEDED");

		PaymentIdentity voidPayment = authorizePayment("m6-void", 10_000L);
		MvcResult voidResponse = mockMvc.perform(post("/payments/{paymentId}/void", voidPayment.paymentId())
				.header("Idempotency-Key", "m6-void-" + UUID.randomUUID()))
				.andExpect(status().isAccepted())
				.andReturn();
		UUID voidOperationId = operationId(voidResponse);
		await("void to complete through Kafka and PSP", () -> "CANCELED".equals(paymentStatus(voidPayment.paymentId())));
		assertThat(operationStatus(voidOperationId)).isEqualTo("SUCCEEDED");
		assertThat(pspDecision(voidOperationId)).containsEntry("result", "SUCCEEDED");

		PaymentIdentity expiredPayment = authorizePayment("m6-expired", 1_680L);
		mockMvc.perform(post("/payments/{paymentId}/capture", expiredPayment.paymentId())
				.header("Idempotency-Key", "m6-expired-" + UUID.randomUUID()))
				.andExpect(status().isConflict());
		assertThat(paymentStatus(expiredPayment.paymentId())).isEqualTo("EXPIRED");
		assertThat(countOperations(expiredPayment.paymentId(), "CAPTURE")).isZero();
		assertThat(auditEventCount(expiredPayment.paymentId(), "PAYMENT_EXPIRED")).isEqualTo(1L);

		PaymentIdentity racedPayment = authorizePayment("m6-race", 10_000L);
		CyclicBarrier start = new CyclicBarrier(2);
		try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
			Future<Integer> captureStatus = executor.submit(() -> {
				start.await();
				return mockMvc.perform(post("/payments/{paymentId}/capture", racedPayment.paymentId())
						.header("Idempotency-Key", "m6-race-capture-" + UUID.randomUUID()))
						.andReturn().getResponse().getStatus();
			});
			Future<Integer> voidStatus = executor.submit(() -> {
				start.await();
				return mockMvc.perform(post("/payments/{paymentId}/void", racedPayment.paymentId())
						.header("Idempotency-Key", "m6-race-void-" + UUID.randomUUID()))
						.andReturn().getResponse().getStatus();
			});
			assertThat(List.of(captureStatus.get(15, TimeUnit.SECONDS), voidStatus.get(15, TimeUnit.SECONDS)))
					.containsExactlyInAnyOrder(202, 409);
		}
		await("the admitted concurrent operation to finish", () ->
				List.of("CAPTURED", "CANCELED").contains(paymentStatus(racedPayment.paymentId())));
		assertThat(countOperations(racedPayment.paymentId(), "CAPTURE")
				+ countOperations(racedPayment.paymentId(), "VOID")).isEqualTo(1L);
	}

	@Test
	@Order(4)
	void realConsumerRestartRedeliversDuplicateWithoutRepeatingTheFinancialEffectOrSkippingItsOffset() throws Exception {
		PaymentIdentity payment = authorizePayment("m6-rebalance", 10_000L);
		long auditCountBefore = auditCount(payment.paymentId());
		double duplicateCountBefore = duplicateReplayCount();
		Map<String, Object> outbox = jdbcTemplate.queryForMap(
				"SELECT * FROM outbox_events WHERE operation_id = ?", payment.operationId());

		MessageListenerContainer listener = listenerRegistry.getListenerContainer(MAIN_LISTENER);
		assertThat(listener).isNotNull();
		listener.stop();
		await("Kafka listener to stop", () -> !listener.isRunning());
		org.springframework.kafka.support.SendResult<String, Object> duplicate;
		try {
			duplicate = kafkaTemplate.send(
					MAIN_TOPIC, payment.paymentId().toString(), commandFrom(outbox)).get(15, TimeUnit.SECONDS);
			listener.start();

			await("duplicate delivery after consumer restart", () -> duplicateReplayCount() > duplicateCountBefore);
			listener.stop();
			await("Kafka listener to commit and stop", () -> !listener.isRunning());
			awaitCommittedOffset(
					duplicate.getRecordMetadata().partition(), duplicate.getRecordMetadata().offset());
		}
		finally {
			if (!listener.isRunning()) {
				listener.start();
			}
		}

		assertThat(auditCount(payment.paymentId())).isEqualTo(auditCountBefore);
		assertThat(count("payment_operations", "id", payment.operationId())).isEqualTo(1L);
		assertThat(pspDecision(payment.operationId())).containsEntry("result", "SUCCEEDED");
	}

	@Test
	@Order(5)
	void expiredKafkaAndTechnicalLogEvidenceCanBeRemovedWhilePostgresAuditReconstructsThePayment() throws Exception {
		PaymentIdentity payment = authorizePayment("m6-retention", 10_000L);
		List<String> durableAudit = auditEvents(payment.paymentId());
		String durableAuditJson = auditJson(payment.paymentId());

		Path expiredTechnicalLog = Files.createTempFile("paymentflow-m6-expired-", ".log");
		Files.writeString(expiredTechnicalLog, "correlationId=" + payment.paymentId());
		Files.delete(expiredTechnicalLog);
		assertThat(expiredTechnicalLog).doesNotExist();

		try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", "localhost:9092"))) {
			Map<TopicPartition, OffsetSpec> latestRequests = new HashMap<>();
			for (int partition = 0; partition < 4; partition++) {
				latestRequests.put(new TopicPartition(MAIN_TOPIC, partition), OffsetSpec.latest());
			}
			Map<TopicPartition, RecordsToDelete> deletions = new HashMap<>();
			admin.listOffsets(latestRequests).all().get(15, TimeUnit.SECONDS)
					.forEach((partition, result) ->
							deletions.put(partition, RecordsToDelete.beforeOffset(result.offset())));
			admin.deleteRecords(deletions).all().get(15, TimeUnit.SECONDS);
		}

		assertThat(auditEvents(payment.paymentId())).containsExactlyElementsOf(durableAudit);
		assertThat(auditJson(payment.paymentId())).isEqualTo(durableAuditJson);
		assertThat(paymentStatus(payment.paymentId())).isEqualTo("AUTHORIZED");
		assertThat(operationStatus(payment.operationId())).isEqualTo("SUCCEEDED");
	}

	private MvcResult admit(String key, String body) throws Exception {
		return mockMvc.perform(post("/payments")
				.header("Idempotency-Key", key)
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
				.andExpect(status().isAccepted())
				.andReturn();
	}

	private String requestBody(String orderPrefix, long amountMinor) {
		return """
				{"orderId":"%s-%s","amount":%d,"currency":"BRL"}
				""".formatted(orderPrefix, UUID.randomUUID(), amountMinor);
	}

	private PaymentIdentity authorizePayment(String orderPrefix, long amountMinor) throws Exception {
		MvcResult response = admit(
				orderPrefix + "-" + UUID.randomUUID(), requestBody(orderPrefix, amountMinor));
		PaymentIdentity identity = new PaymentIdentity(paymentId(response), operationId(response));
		await(orderPrefix + " authorization", () -> "AUTHORIZED".equals(paymentStatus(identity.paymentId())));
		return identity;
	}

	private UUID paymentId(MvcResult result) throws Exception {
		return UUID.fromString(JsonPath.read(result.getResponse().getContentAsString(), "$.paymentId"));
	}

	private UUID operationId(MvcResult result) throws Exception {
		return UUID.fromString(JsonPath.read(result.getResponse().getContentAsString(), "$.operationId"));
	}

	private String paymentStatus(UUID paymentId) {
		return jdbcTemplate.queryForObject("SELECT status FROM payments WHERE id = ?", String.class, paymentId);
	}

	private String operationStatus(UUID operationId) {
		return jdbcTemplate.queryForObject(
				"SELECT status FROM payment_operations WHERE id = ?", String.class, operationId);
	}

	private int attemptCount(UUID operationId) {
		return jdbcTemplate.queryForObject(
				"SELECT attempt_count FROM payment_operations WHERE id = ?", Integer.class, operationId);
	}

	private String outboxStatus(UUID operationId) {
		return jdbcTemplate.queryForObject(
				"SELECT status FROM outbox_events WHERE operation_id = ?", String.class, operationId);
	}

	private long count(String table, String column, UUID value) {
		return jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM " + table + " WHERE " + column + " = ?", Long.class, value);
	}

	private long auditEventCount(UUID paymentId, String eventType) {
		return jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM payment_audit_events WHERE payment_id = ? AND event_type = ?",
				Long.class, paymentId, eventType);
	}

	private long auditCount(UUID paymentId) {
		return jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM payment_audit_events WHERE payment_id = ?", Long.class, paymentId);
	}

	private long countOperations(UUID paymentId, String operationType) {
		return jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM payment_operations WHERE payment_id = ? AND type = ?",
				Long.class, paymentId, operationType);
	}

	private double duplicateReplayCount() {
		return meterRegistry.find("paymentflow.duplicate.replay")
				.counters().stream().mapToDouble(counter -> counter.count()).sum();
	}

	private GenericRecord commandFrom(Map<String, Object> outbox) {
		GenericRecord command = new GenericData.Record(executePaymentOperationSchema);
		command.put("messageId", outbox.get("message_id").toString());
		command.put("commandType", outbox.get("command_type").toString());
		command.put("operationId", outbox.get("operation_id").toString());
		command.put("correlationId", outbox.get("correlation_id").toString());
		Object occurredAt = outbox.get("occurred_at");
		Instant occurredAtInstant = occurredAt instanceof OffsetDateTime offsetDateTime
				? offsetDateTime.toInstant()
				: ((java.sql.Timestamp) occurredAt).toInstant();
		command.put("occurredAt", occurredAtInstant.toEpochMilli());
		command.put("causationId", null);
		return command;
	}

	private void awaitCommittedOffset(int partition, long processedOffset) throws Exception {
		try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", "localhost:9092"))) {
			long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
			while (System.nanoTime() < deadline) {
				org.apache.kafka.clients.consumer.OffsetAndMetadata metadata =
						admin.listConsumerGroupOffsets(MAIN_LISTENER).partitionsToOffsetAndMetadata()
								.get(10, TimeUnit.SECONDS)
								.get(new TopicPartition(MAIN_TOPIC, partition));
				long committed = metadata == null ? -1L : metadata.offset();
				if (committed > processedOffset) {
					return;
				}
				Thread.sleep(250L);
			}
		}
		throw new AssertionError("Timed out waiting for committed group offset after " + processedOffset);
	}

	private java.util.List<String> auditEvents(UUID paymentId) {
		return jdbcTemplate.queryForList(
				"SELECT event_type FROM payment_audit_events WHERE payment_id = ? ORDER BY sequence_number",
				String.class, paymentId);
	}

	private String auditJson(UUID paymentId) {
		return jdbcTemplate.queryForObject("""
				SELECT COALESCE(string_agg(row_to_json(a)::text, ''), '')
				FROM payment_audit_events a
				WHERE payment_id = ?
				""", String.class, paymentId);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> pspDecision(UUID operationId) {
		return psp.get()
				.uri("/simulator/operations/{id}", operationId)
				.retrieve()
				.body(Map.class);
	}

	private void await(String description, Supplier<Boolean> condition) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
		while (System.nanoTime() < deadline) {
			if (condition.get()) {
				return;
			}
			Thread.sleep(100L);
		}
		throw new AssertionError("Timed out waiting for " + description);
	}

	private record PaymentIdentity(UUID paymentId, UUID operationId) {
	}
}
