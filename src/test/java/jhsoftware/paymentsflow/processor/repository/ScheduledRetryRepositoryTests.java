package jhsoftware.paymentsflow.processor.repository;

import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.UnresolvedRetryLineageException;
import jhsoftware.paymentsflow.processor.messaging.PaymentOperationRetryIdentity;
import jhsoftware.paymentsflow.processor.model.ScheduledRetry;
import jhsoftware.paymentsflow.processor.service.RetryBackoffPolicy;
import jhsoftware.paymentsflow.processor.service.TechnicalFailureClassifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
class ScheduledRetryRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private JdbcClient jdbcClient;
	private ScheduledRetryRepository repository;

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
		repository = new ScheduledRetryRepository(jdbcClient, new RetryBackoffPolicy(() -> 0.5));
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM scheduled_retries").update();
		jdbcClient.sql("DELETE FROM outbox_events").update();
		jdbcClient.sql("DELETE FROM payment_operations").update();
		jdbcClient.sql("DELETE FROM payments").update();
	}

	@Test
	void originalOutboxPublicationCreatesFirstPendingScheduleUsingDatabaseTime() {
		UUID messageId = seedOriginalPublication();
		PaymentOperationRetryEnvelopeDto envelope = envelope(1L, messageId);

		ScheduledRetry schedule = repository.persist(envelope, 17);

		assertThat(schedule.retryNumber()).isEqualTo(1);
		assertThat(schedule.nextEligibleAt()).isEqualTo(schedule.scheduledAt().plusSeconds(10));
		assertThat(jdbcClient.sql("SELECT state FROM scheduled_retries WHERE retry_id = :retryId")
				.param("retryId", envelope.retryId()).query(String.class).single()).isEqualTo("PENDING");
	}

	@Test
	void duplicateKeepsTheFirstRetryNumberAndEligibilityEvenWhenDiagnosticsChange() {
		UUID messageId = seedOriginalPublication();
		PaymentOperationRetryEnvelopeDto first = envelope(2L, messageId);
		ScheduledRetry stored = repository.persist(first, 23);
		PaymentOperationRetryEnvelopeDto redelivery = new PaymentOperationRetryEnvelopeDto(
				first.retryId(), first.sourceTopic(), first.sourcePartition(), first.sourceOffset(),
				first.sourceRecordKey(), first.sourceRecordValue(), "A_LATER_FAILURE", first.failedAt().plusSeconds(30),
				first.operationId(), first.correlationId(), first.sourceMessageId());

		ScheduledRetry duplicate = repository.persist(redelivery, 23);

		assertThat(duplicate).isEqualTo(stored);
		assertThat(jdbcClient.sql("SELECT count(*) FROM scheduled_retries")
				.query(Long.class).single()).isEqualTo(1L);
	}

	@Test
	void laterPublicationDerivesTheNextRetryNumberFromItsParentSchedule() {
		UUID originalMessageId = seedOriginalPublication();
		PaymentOperationRetryEnvelopeDto parent = envelope(3L, originalMessageId);
		repository.persist(parent, 31);
		UUID dispatchMessageId = UUID.randomUUID();
		jdbcClient.sql("UPDATE scheduled_retries SET dispatch_message_id = :messageId WHERE retry_id = :retryId")
				.param("messageId", dispatchMessageId)
				.param("retryId", parent.retryId())
				.update();

		ScheduledRetry child = repository.persist(envelope(4L, dispatchMessageId), 31);

		assertThat(child.retryNumber()).isEqualTo(2);
		assertThat(child.nextEligibleAt()).isEqualTo(child.scheduledAt().plusSeconds(20));
	}

	@Test
	void unknownLineageIsHeldWithoutCreatingASchedule() {
		PaymentOperationRetryEnvelopeDto envelope = envelope(5L, UUID.randomUUID());

		assertThatThrownBy(() -> repository.persist(envelope, 11))
				.isInstanceOf(UnresolvedRetryLineageException.class);
		assertThat(jdbcClient.sql("SELECT count(*) FROM scheduled_retries")
				.query(Long.class).single()).isZero();
	}

	@Test
	void backlogStatsAnchorsOldestPendingAgeOnCreationTimeNotNextEligibleTime() {
		Instant now = Instant.parse("2026-10-27T12:00:00Z");
		insertScheduledRetry("topic-a", 0, now.minusSeconds(10_000), now.plusSeconds(5_000));
		insertScheduledRetry("topic-b", 0, now.minusSeconds(100), now.minusSeconds(50));

		ScheduledRetryRepository.BacklogStats stats = repository.backlogStats(now);

		assertThat(stats.backlogCount()).isEqualTo(2L);
		assertThat(stats.oldestPendingAgeSeconds()).isEqualTo(10_000L);
	}

	private void insertScheduledRetry(String sourceTopic, int sourcePartition, Instant createdAt, Instant nextEligibleAt) {
		jdbcClient.sql("""
				INSERT INTO scheduled_retries
				    (retry_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, source_schema_id,
				     failure_class, failed_at, retry_number, scheduled_at, next_eligible_at, state,
				     claim_generation, created_at, updated_at)
				VALUES
				    (:retryId, :sourceTopic, :sourcePartition, 0,
				     :sourceRecordKey, :sourceRecordValue, 1,
				     :failureClass, :createdAt, 1, :createdAt, :nextEligibleAt, 'PENDING',
				     0, :createdAt, :createdAt)
				""")
				.param("retryId", UUID.randomUUID())
				.param("sourceTopic", sourceTopic)
				.param("sourcePartition", sourcePartition)
				.param("sourceRecordKey", new byte[] {1})
				.param("sourceRecordValue", new byte[] {2})
				.param("failureClass", TechnicalFailureClassifier.DATA_ACCESS_UNAVAILABLE)
				.param("createdAt", createdAt.atOffset(ZoneOffset.UTC))
				.param("nextEligibleAt", nextEligibleAt.atOffset(ZoneOffset.UTC))
				.update();
	}

	@Test
	void conflictingImmutableEvidenceIsRejected() {
		UUID messageId = seedOriginalPublication();
		PaymentOperationRetryEnvelopeDto first = envelope(6L, messageId);
		repository.persist(first, 41);
		PaymentOperationRetryEnvelopeDto conflicting = new PaymentOperationRetryEnvelopeDto(
				first.retryId(), first.sourceTopic(), first.sourcePartition(), first.sourceOffset(),
				first.sourceRecordKey(), new byte[] {9}, first.failureClass(), first.failedAt(),
				first.operationId(), first.correlationId(), first.sourceMessageId());

		assertThatThrownBy(() -> repository.persist(conflicting, 41))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Conflicting immutable retry identity");
	}

	private UUID seedOriginalPublication() {
		UUID paymentId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID messageId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
		jdbcClient.sql("""
				INSERT INTO payments
				    (id, merchant_id, external_order_id, amount_minor, currency, status, created_at, updated_at)
				VALUES (:id, :merchantId, :orderId, 5000, 'BRL', 'CREATED', :now, :now)
				""")
				.param("id", paymentId)
				.param("merchantId", UUID.randomUUID())
				.param("orderId", "order-" + paymentId)
				.param("now", now)
				.update();
		jdbcClient.sql("""
				INSERT INTO payment_operations
				    (id, payment_id, type, status, amount_minor, psp_idempotency_key,
				     attempt_count, created_at, updated_at)
				VALUES (:id, :paymentId, 'AUTHORIZE', 'PENDING', 5000, :pspKey, 0, :now, :now)
				""")
				.param("id", operationId)
				.param("paymentId", paymentId)
				.param("pspKey", "psp-" + operationId)
				.param("now", now)
				.update();
		jdbcClient.sql("""
				INSERT INTO outbox_events
				    (message_id, payment_id, operation_id, command_type, correlation_id,
				     occurred_at, status, created_at)
				VALUES (:messageId, :paymentId, :operationId, 'ExecutePaymentOperation', :correlationId,
				        :now, 'PENDING', :now)
				""")
				.param("messageId", messageId)
				.param("paymentId", paymentId)
				.param("operationId", operationId)
				.param("correlationId", correlationId)
				.param("now", now)
				.update();
		return messageId;
	}

	private PaymentOperationRetryEnvelopeDto envelope(long offset, UUID sourceMessageId) {
		String topic = "payment.operations.requested.v1";
		return new PaymentOperationRetryEnvelopeDto(
				PaymentOperationRetryIdentity.retryId(topic, 1, offset),
				topic,
				1,
				offset,
				UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
				new byte[] {0, 0, 0, 0, 17, 1, 2},
				TechnicalFailureClassifier.DATA_ACCESS_UNAVAILABLE,
				Instant.parse("2026-10-27T10:00:00Z"),
				UUID.randomUUID(),
				UUID.randomUUID(),
				sourceMessageId);
	}
}
