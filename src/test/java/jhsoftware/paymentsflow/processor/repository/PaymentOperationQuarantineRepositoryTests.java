package jhsoftware.paymentsflow.processor.repository;

import jhsoftware.paymentsflow.processor.dto.PaymentOperationQuarantineEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
class PaymentOperationQuarantineRepositoryTests {

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

	private static HikariDataSource dataSource;
	private JdbcClient jdbcClient;
	private PaymentOperationQuarantineRepository repository;
	private SimpleMeterRegistry meterRegistry;

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
		meterRegistry = new SimpleMeterRegistry();
		repository = new PaymentOperationQuarantineRepository(jdbcClient, meterRegistry);
	}

	@AfterEach
	void cleanDatabase() {
		jdbcClient.sql("DELETE FROM quarantined_commands").update();
	}

	@Test
	void duplicateDeliveryFindsOneImmutableQuarantineRecord() {
		PaymentOperationQuarantineEnvelopeDto envelope = envelope(new byte[] {1, 2, 3});

		repository.persist(envelope);
		repository.persist(envelope);

		assertThat(jdbcClient.sql("SELECT count(*) FROM quarantined_commands")
				.query(Long.class).single()).isEqualTo(1L);
		assertThat(meterRegistry.get("paymentflow.dlq.entries")
				.tag("failure_code", PoisonFailureCode.INVALID_AVRO_PAYLOAD.name())
				.counter().count()).isEqualTo(2.0);
	}

	@Test
	void sameIdentityWithDifferentEvidenceIsRejected() {
		repository.persist(envelope(new byte[] {1, 2, 3}));

		assertThatThrownBy(() -> repository.persist(envelope(new byte[] {9, 9, 9})))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Conflicting immutable quarantine identity");
	}

	private PaymentOperationQuarantineEnvelopeDto envelope(byte[] value) {
		return new PaymentOperationQuarantineEnvelopeDto(
				UUID.fromString("11111111-1111-5111-9111-111111111111"),
				"payment.operations.requested.v1",
				2,
				17L,
				new byte[] {4, 5},
				value,
				PoisonFailureCode.INVALID_AVRO_PAYLOAD.name(),
				Instant.parse("2026-10-27T10:00:00Z"),
				null,
				null,
				null);
	}
}
