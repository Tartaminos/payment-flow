package jhsoftware.paymentsflow.processor.service;

import jhsoftware.paymentsflow.processor.messaging.PaymentOperationCommandListener;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetrySchedulerListener;
import jhsoftware.paymentsflow.processor.model.DueScheduledRetry;
import jhsoftware.paymentsflow.processor.repository.ScheduledRetryRepository;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import jhsoftware.paymentsflow.outbox.OutboxMessagingConfiguration;

@EnabledIfSystemProperty(named = "paymentflow.real-messaging-test", matches = "true")
@Testcontainers
@SpringBootTest(properties = {
		"spring.docker.compose.enabled=false",
		"spring.kafka.bootstrap-servers=localhost:9092",
		"spring.kafka.producer.properties.schema.registry.url=http://localhost:8082",
		"paymentflow.messaging.enabled=true",
		"paymentflow.messaging.schema-registry-url=http://localhost:8082",
		"paymentflow.messaging.publisher.poll-interval=1s",
		"paymentflow.processor.dispatcher.poll-interval=1h"
})
class ScheduledRetryDispatcherRealMessagingIntegrationTests {

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	DataSource dataSource;

	@Autowired
	ScheduledRetryRepository repository;

	@Autowired
	ScheduledRetryDispatcher dispatcher;

	@Autowired
	RetryBackoffPolicy backoffPolicy;

	@Autowired
	KafkaListenerEndpointRegistry listenerRegistry;

	@BeforeEach
	void stopLiveCommandListener() {
		stopContainer(PaymentOperationCommandListener.LISTENER_ID);
		stopContainer(PaymentOperationRetrySchedulerListener.LISTENER_ID);
	}

	private void stopContainer(String listenerId) {
		MessageListenerContainer container = listenerRegistry.getListenerContainer(listenerId);
		if (container != null && container.isRunning()) {
			container.stop();
		}
	}

	@BeforeEach
	void cleanDatabase() {
		jdbcTemplate.update("DELETE FROM outbox_events");
		jdbcTemplate.update("DELETE FROM scheduled_retries");
		jdbcTemplate.update("DELETE FROM payment_audit_events");
		jdbcTemplate.update("DELETE FROM payment_operations");
		jdbcTemplate.update("DELETE FROM payments");
	}

	@Test
	void dispatchedScheduleIsRepublishedWithPreservedLineageAndPaymentKeyedRouting() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID sourceMessageId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		UUID retryId = seedScheduledRetry(1, operationId, correlationId, sourceMessageId, Instant.now().minusSeconds(5));

		try (KafkaConsumer<byte[], Object> consumer = consumer("m5-04-happy-path")) {
			consumer.subscribe(List.of(OutboxMessagingConfiguration.TOPIC));
			awaitAssignment(consumer);

			dispatcher.dispatchDue();

			assertThat(stateOf(retryId)).isEqualTo("DISPATCHED");
			UUID dispatchMessageId = dispatchMessageIdOf(retryId);
			assertThat(dispatchMessageId).isNotNull().isNotEqualTo(sourceMessageId);

			ConsumerRecord<byte[], Object> published = awaitMessage(consumer, dispatchMessageId);
			assertThat(new String(published.key(), StandardCharsets.UTF_8)).isEqualTo(paymentId.toString());

			GenericRecord command = (GenericRecord) published.value();
			assertThat(command.get("messageId").toString()).isEqualTo(dispatchMessageId.toString());
			assertThat(command.get("operationId").toString()).isEqualTo(operationId.toString());
			assertThat(command.get("correlationId").toString()).isEqualTo(correlationId.toString());
			assertThat(command.get("causationId").toString()).isEqualTo(sourceMessageId.toString());

			assertThat(jdbcTemplate.queryForObject(
					"SELECT count(*) FROM outbox_events WHERE operation_id = ?", Long.class, operationId))
					.isEqualTo(1L);
			assertThat(jdbcTemplate.queryForObject(
					"SELECT payment_id FROM outbox_events WHERE message_id = ?", UUID.class, dispatchMessageId))
					.isEqualTo(paymentId);
		}
	}

	@Test
	void redispatchAttemptAfterPublicationIsNotReclaimedAndNeverProducesASecondMainTopicCommand() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		UUID sourceMessageId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		UUID retryId = seedScheduledRetry(2, operationId, correlationId, sourceMessageId, Instant.now().minusSeconds(5));

		UUID firstWorker = UUID.randomUUID();
		Instant now = Instant.now();
		List<DueScheduledRetry> firstClaim = repository.claimDue(20, firstWorker, now, now.plusSeconds(60));
		assertThat(firstClaim).extracting(DueScheduledRetry::retryId).containsExactly(retryId);
		repository.dispatch(firstClaim.get(0), firstWorker, now);
		UUID dispatchMessageId = dispatchMessageIdOf(retryId);

		try (KafkaConsumer<byte[], Object> consumer = consumer("m5-04-redispatch")) {
			consumer.subscribe(List.of(OutboxMessagingConfiguration.TOPIC));
			awaitAssignment(consumer);

			UUID secondWorker = UUID.randomUUID();
			Instant later = now.plusSeconds(10);
			List<DueScheduledRetry> secondClaim = repository.claimDue(20, secondWorker, later, later.plusSeconds(60));

			assertThat(secondClaim)
					.as("an already-DISPATCHED schedule must never be reclaimed via the normal claim path")
					.extracting(DueScheduledRetry::retryId)
					.doesNotContain(retryId);

			ConsumerRecord<byte[], Object> firstPublication = awaitMessage(consumer, dispatchMessageId);
			assertThat(firstPublication.value()).isNotNull();

			List<ConsumerRecord<byte[], Object>> duplicateCommands = pollFor(
					consumer, record -> operationId.toString().equals(((GenericRecord) record.value()).get("operationId").toString())
							&& !dispatchMessageId.toString().equals(((GenericRecord) record.value()).get("messageId").toString()),
					Duration.ofSeconds(3));
			assertThat(duplicateCommands)
					.as("redispatching the same retry_id must not create a second distinct main-topic command")
					.isEmpty();
			assertThat(jdbcTemplate.queryForObject(
					"SELECT count(*) FROM outbox_events WHERE operation_id = ?", Long.class, operationId))
					.isEqualTo(1L);
		}
	}

	@Test
	void concurrentClaimDueCallsNeverDoubleClaimAndEachRetryYieldsExactlyOneOutboxRow() throws Exception {
		int seededCount = 6;
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		seedPaymentAndOperation(paymentId, operationId);
		List<UUID> retryIds = new ArrayList<>();
		for (int index = 0; index < seededCount; index++) {
			retryIds.add(seedScheduledRetry(
					100 + index, operationId, UUID.randomUUID(), UUID.randomUUID(), Instant.now().minusSeconds(5)));
		}

		ScheduledRetryRepository repositoryA = new ScheduledRetryRepository(JdbcClient.create(dataSource), backoffPolicy);
		ScheduledRetryRepository repositoryB = new ScheduledRetryRepository(JdbcClient.create(dataSource), backoffPolicy);
		CountDownLatch startSignal = new CountDownLatch(1);
		CopyOnWriteArrayList<UUID> claimedByA = new CopyOnWriteArrayList<>();
		CopyOnWriteArrayList<UUID> claimedByB = new CopyOnWriteArrayList<>();

		ExecutorService executor = Executors.newFixedThreadPool(2);
		Future<?> taskA = executor.submit(() -> runClaimAndDispatchLoop(repositoryA, startSignal, claimedByA));
		Future<?> taskB = executor.submit(() -> runClaimAndDispatchLoop(repositoryB, startSignal, claimedByB));
		startSignal.countDown();
		taskA.get(30, TimeUnit.SECONDS);
		taskB.get(30, TimeUnit.SECONDS);
		executor.shutdown();
		executor.awaitTermination(10, TimeUnit.SECONDS);

		Set<UUID> overlap = new java.util.HashSet<>(claimedByA);
		overlap.retainAll(claimedByB);
		assertThat(overlap).as("no retry_id must be claimed by both concurrent callers").isEmpty();
		Set<UUID> union = new java.util.HashSet<>(claimedByA);
		union.addAll(claimedByB);
		assertThat(union).containsExactlyInAnyOrderElementsOf(retryIds);

		for (UUID retryId : retryIds) {
			assertThat(stateOf(retryId)).isEqualTo("DISPATCHED");
		}
		assertThat(jdbcTemplate.queryForObject(
				"SELECT count(*) FROM outbox_events WHERE operation_id = ?", Long.class, operationId))
				.isEqualTo((long) seededCount);
		assertThat(jdbcTemplate.queryForObject(
				"SELECT count(DISTINCT message_id) FROM outbox_events WHERE operation_id = ?", Long.class, operationId))
				.isEqualTo((long) seededCount);
	}

	private void runClaimAndDispatchLoop(
			ScheduledRetryRepository repository, CountDownLatch startSignal, CopyOnWriteArrayList<UUID> claimed) {
		try {
			startSignal.await();
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			return;
		}
		UUID workerId = UUID.randomUUID();
		for (int iteration = 0; iteration < 5; iteration++) {
			Instant now = Instant.now();
			List<DueScheduledRetry> batch = repository.claimDue(2, workerId, now, now.plusSeconds(60));
			if (batch.isEmpty()) {
				return;
			}
			for (DueScheduledRetry retry : batch) {
				repository.dispatch(retry, workerId, now);
				claimed.add(retry.retryId());
			}
		}
	}

	private List<ConsumerRecord<byte[], Object>> pollFor(
			KafkaConsumer<byte[], Object> consumer,
			java.util.function.Predicate<ConsumerRecord<byte[], Object>> matcher,
			Duration window) {
		List<ConsumerRecord<byte[], Object>> matches = new ArrayList<>();
		long deadline = System.nanoTime() + window.toNanos();
		while (System.nanoTime() < deadline) {
			for (ConsumerRecord<byte[], Object> candidate : consumer.poll(Duration.ofMillis(500))) {
				if (matcher.test(candidate)) {
					matches.add(candidate);
				}
			}
		}
		return matches;
	}

	private UUID dispatchMessageIdOf(UUID retryId) {
		return jdbcTemplate.queryForObject(
				"SELECT dispatch_message_id FROM scheduled_retries WHERE retry_id = ?", UUID.class, retryId);
	}

	private String stateOf(UUID retryId) {
		return jdbcTemplate.queryForObject(
				"SELECT state FROM scheduled_retries WHERE retry_id = ?", String.class, retryId);
	}

	private UUID seedScheduledRetry(
			long offset, UUID operationId, UUID correlationId, UUID sourceMessageId, Instant nextEligibleAt) {
		UUID retryId = UUID.randomUUID();
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		jdbcTemplate.update("""
				INSERT INTO scheduled_retries
				    (retry_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, source_schema_id,
				     failure_class, failed_at, operation_id, correlation_id, source_message_id,
				     retry_number, scheduled_at, next_eligible_at, state,
				     claim_generation, created_at, updated_at)
				VALUES
				    (?, 'payment.operations.requested.v1', 9, ?,
				     ?, ?, 1,
				     'DATA_ACCESS_UNAVAILABLE', ?, ?, ?, ?,
				     1, ?, ?, 'PENDING',
				     0, ?, ?)
				""",
				retryId, offset,
				new byte[] {1}, new byte[] {1, 2, 3},
				now, operationId, correlationId, sourceMessageId,
				OffsetDateTime.ofInstant(nextEligibleAt, ZoneOffset.UTC),
				OffsetDateTime.ofInstant(nextEligibleAt, ZoneOffset.UTC),
				now, now);
		return retryId;
	}

	private void seedPaymentAndOperation(UUID paymentId, UUID operationId) {
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		jdbcTemplate.update("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, created_at, updated_at)
				VALUES (?, ?, ?, 5000, 'BRL', 'CREATED', ?, ?)
				""", paymentId, UUID.randomUUID(), "order-" + paymentId, now, now);
		jdbcTemplate.update("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count,
				     created_at, updated_at)
				VALUES (?, ?, 'AUTHORIZE', 'PENDING', 5000, ?, 0, ?, ?)
				""", operationId, paymentId, "psp-" + operationId, now, now);
	}

	private KafkaConsumer<byte[], Object> consumer(String groupPrefix) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupPrefix + "-" + UUID.randomUUID());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		properties.put("schema.registry.url", "http://localhost:8082");
		return new KafkaConsumer<>(properties);
	}

	private void awaitAssignment(KafkaConsumer<byte[], Object> consumer) {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
			consumer.poll(Duration.ofMillis(250));
		}
		assertThat(consumer.assignment()).as("topic partition assignment").isNotEmpty();
	}

	private ConsumerRecord<byte[], Object> awaitMessage(KafkaConsumer<byte[], Object> consumer, UUID expectedMessageId) {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (System.nanoTime() < deadline) {
			for (ConsumerRecord<byte[], Object> record : consumer.poll(Duration.ofMillis(500))) {
				GenericRecord command = (GenericRecord) record.value();
				if (expectedMessageId.toString().equals(command.get("messageId").toString())) {
					return record;
				}
			}
		}
		throw new AssertionError("Expected Kafka command was not observed within 20 seconds");
	}
}
