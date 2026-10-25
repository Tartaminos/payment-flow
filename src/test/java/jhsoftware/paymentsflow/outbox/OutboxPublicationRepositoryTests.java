package jhsoftware.paymentsflow.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
class OutboxPublicationRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private OutboxPublicationRepository repository;

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
		repository = new OutboxPublicationRepository(JdbcClient.create(dataSource));
	}

	@AfterEach
	void cleanDatabase() {
		JdbcClient.create(dataSource).sql("DELETE FROM outbox_events").update();
		JdbcClient.create(dataSource).sql("DELETE FROM payment_operations").update();
		JdbcClient.create(dataSource).sql("DELETE FROM payments").update();
	}

	@Test
	void claimReturnsOnlyEligiblePendingRowsAndMarksThemPublishing() {
		UUID messageId = seedOutboxEvent();
		UUID workerId = UUID.randomUUID();
		Instant now = Instant.now();

		List<OutboxPublication> claimed = repository.claimPending(20, workerId, now, now.plusSeconds(60));

		assertThat(claimed).extracting(OutboxPublication::messageId).containsExactly(messageId);
		assertThat(claimed.get(0).attemptCount()).isEqualTo(1);
		assertThat(statusOf(messageId)).isEqualTo("PUBLISHING");
	}

	@Test
	void claimedRowIsNotReturnedAgainWhileLeaseIsStillValid() {
		seedOutboxEvent();
		Instant now = Instant.now();
		repository.claimPending(20, UUID.randomUUID(), now, now.plusSeconds(60));

		List<OutboxPublication> secondClaim = repository.claimPending(20, UUID.randomUUID(), now, now.plusSeconds(60));

		assertThat(secondClaim).isEmpty();
	}

	@Test
	void expiredLeaseIsReclaimableByAnotherWorker() {
		UUID messageId = seedOutboxEvent();
		UUID staleWorker = UUID.randomUUID();
		UUID newWorker = UUID.randomUUID();
		Instant past = Instant.now().minusSeconds(120);
		repository.claimPending(20, staleWorker, past, past.plusSeconds(60));

		Instant now = Instant.now();
		List<OutboxPublication> reclaimed = repository.claimPending(20, newWorker, now, now.plusSeconds(60));

		assertThat(reclaimed).extracting(OutboxPublication::messageId).containsExactly(messageId);
		assertThat(reclaimed.get(0).attemptCount()).isEqualTo(2);
	}

	@Test
	void stalePublisherCannotMarkPublishedAfterLeaseWasReclaimed() {
		UUID messageId = seedOutboxEvent();
		UUID staleWorker = UUID.randomUUID();
		UUID newWorker = UUID.randomUUID();
		Instant past = Instant.now().minusSeconds(120);
		repository.claimPending(20, staleWorker, past, past.plusSeconds(60));
		Instant now = Instant.now();
		repository.claimPending(20, newWorker, now, now.plusSeconds(60));

		boolean staleUpdateApplied = repository.markPublished(messageId, staleWorker, now);

		assertThat(staleUpdateApplied).isFalse();
		assertThat(statusOf(messageId)).isEqualTo("PUBLISHING");
	}

	@Test
	void stalePublisherCannotReleaseAfterLeaseWasReclaimed() {
		UUID messageId = seedOutboxEvent();
		UUID staleWorker = UUID.randomUUID();
		UUID newWorker = UUID.randomUUID();
		Instant past = Instant.now().minusSeconds(120);
		repository.claimPending(20, staleWorker, past, past.plusSeconds(60));
		Instant now = Instant.now();
		repository.claimPending(20, newWorker, now, now.plusSeconds(60));

		boolean staleReleaseApplied = repository.releaseForRetry(messageId, staleWorker, now.plusSeconds(5));

		assertThat(staleReleaseApplied).isFalse();
		assertThat(statusOf(messageId)).isEqualTo("PUBLISHING");
	}

	@Test
	void ownerCanMarkPublishedAndClearsClaimAndLease() {
		UUID messageId = seedOutboxEvent();
		UUID workerId = UUID.randomUUID();
		Instant now = Instant.now();
		repository.claimPending(20, workerId, now, now.plusSeconds(60));

		boolean applied = repository.markPublished(messageId, workerId, now);

		assertThat(applied).isTrue();
		assertThat(statusOf(messageId)).isEqualTo("PUBLISHED");
	}

	@Test
	void releasedRowIsNotEligibleAgainUntilBackoffElapses() {
		UUID messageId = seedOutboxEvent();
		UUID workerId = UUID.randomUUID();
		Instant now = Instant.now();
		repository.claimPending(20, workerId, now, now.plusSeconds(60));
		repository.releaseForRetry(messageId, workerId, now.plusSeconds(30));

		List<OutboxPublication> tooEarly = repository.claimPending(20, UUID.randomUUID(), now.plusSeconds(1), now.plusSeconds(61));
		List<OutboxPublication> afterBackoff = repository.claimPending(
				20, UUID.randomUUID(), now.plusSeconds(31), now.plusSeconds(91));

		assertThat(tooEarly).isEmpty();
		assertThat(afterBackoff).extracting(OutboxPublication::messageId).containsExactly(messageId);
	}

	@Test
	void concurrentWorkersNeverClaimTheSameRowTwice() throws Exception {
		List<UUID> messageIds = IntStream.range(0, 20).mapToObj(index -> seedOutboxEvent())
				.collect(Collectors.toList());

		ExecutorService executor = Executors.newFixedThreadPool(5);
		CountDownLatch startGate = new CountDownLatch(1);
		List<Future<List<OutboxPublication>>> futures = IntStream.range(0, 5)
				.mapToObj(index -> executor.submit(() -> {
					startGate.await();
					Instant now = Instant.now();
					return repository.claimPending(20, UUID.randomUUID(), now, now.plusSeconds(60));
				}))
				.collect(Collectors.toList());
		startGate.countDown();

		List<UUID> claimedAcrossWorkers = futures.stream()
				.map(this::join)
				.flatMap(List::stream)
				.map(OutboxPublication::messageId)
				.collect(Collectors.toList());
		executor.shutdown();
		executor.awaitTermination(10, TimeUnit.SECONDS);

		assertThat(claimedAcrossWorkers).hasSize(messageIds.size());
		assertThat(claimedAcrossWorkers).doesNotHaveDuplicates();
		assertThat(claimedAcrossWorkers).containsExactlyInAnyOrderElementsOf(messageIds);
	}

	private <T> T join(Future<T> future) {
		try {
			return future.get(10, TimeUnit.SECONDS);
		}
		catch (Exception exception) {
			throw new RuntimeException(exception);
		}
	}

	private String statusOf(UUID messageId) {
		return JdbcClient.create(dataSource).sql("SELECT status FROM outbox_events WHERE message_id = :messageId")
				.param("messageId", messageId)
				.query(String.class)
				.single();
	}

	private UUID seedOutboxEvent() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement payment = connection.prepareStatement("""
					INSERT INTO payments
						(id, merchant_id, external_order_id, amount_minor, currency, status, created_at, updated_at)
					VALUES (?, ?, ?, 5000, 'BRL', 'CREATED', ?, ?)
					""")) {
				payment.setObject(1, paymentId);
				payment.setObject(2, UUID.randomUUID());
				payment.setString(3, "order-" + paymentId);
				payment.setObject(4, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				payment.setObject(5, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				payment.executeUpdate();
			}
			try (PreparedStatement operation = connection.prepareStatement("""
					INSERT INTO payment_operations
						(id, payment_id, type, status, amount_minor, psp_idempotency_key, attempt_count,
						 created_at, updated_at)
					VALUES (?, ?, 'AUTHORIZE', 'PENDING', 5000, ?, 0, ?, ?)
					""")) {
				operation.setObject(1, operationId);
				operation.setObject(2, paymentId);
				operation.setString(3, "psp-key-" + operationId);
				operation.setObject(4, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				operation.setObject(5, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				operation.executeUpdate();
			}
			try (PreparedStatement outbox = connection.prepareStatement("""
					INSERT INTO outbox_events
						(message_id, payment_id, operation_id, command_type, correlation_id, causation_id,
						 occurred_at, status, created_at)
					VALUES (?, ?, ?, 'ExecutePaymentOperation', ?, NULL, ?, 'PENDING', ?)
					""")) {
				outbox.setObject(1, messageId);
				outbox.setObject(2, paymentId);
				outbox.setObject(3, operationId);
				outbox.setObject(4, UUID.randomUUID());
				outbox.setObject(5, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				outbox.setObject(6, OffsetDateTime.ofInstant(now, ZoneOffset.UTC));
				outbox.executeUpdate();
			}
		}
		catch (Exception exception) {
			throw new RuntimeException(exception);
		}
		return messageId;
	}
}
