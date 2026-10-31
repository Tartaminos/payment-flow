package jhsoftware.paymentsflow.processor.repository;

import jhsoftware.paymentsflow.processor.dto.PaymentOperationQuarantineEnvelopeDto;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
public class PaymentOperationQuarantineRepository {

	private final JdbcClient jdbcClient;
	private final MeterRegistry meterRegistry;

	PaymentOperationQuarantineRepository(JdbcClient jdbcClient, MeterRegistry meterRegistry) {
		this.jdbcClient = jdbcClient;
		this.meterRegistry = meterRegistry;
	}

	@Transactional
	public void persist(PaymentOperationQuarantineEnvelopeDto envelope) {
		jdbcClient.sql("""
				INSERT INTO quarantined_commands
				    (quarantine_id, source_topic, source_partition, source_offset,
				     source_record_key, source_record_value, failure_code, quarantined_at,
				     operation_id, correlation_id, source_message_id, created_at)
				VALUES
				    (:quarantineId, :sourceTopic, :sourcePartition, :sourceOffset,
				     :sourceRecordKey, :sourceRecordValue, :failureCode, :quarantinedAt,
				     :operationId, :correlationId, :sourceMessageId, :quarantinedAt)
				ON CONFLICT DO NOTHING
				""")
				.param("quarantineId", envelope.quarantineId())
				.param("sourceTopic", envelope.sourceTopic())
				.param("sourcePartition", envelope.sourcePartition())
				.param("sourceOffset", envelope.sourceOffset())
				.param("sourceRecordKey", envelope.sourceRecordKey())
				.param("sourceRecordValue", envelope.sourceRecordValue())
				.param("failureCode", envelope.failureCode())
				.param("quarantinedAt", OffsetDateTime.ofInstant(envelope.quarantinedAt(), ZoneOffset.UTC))
				.param("operationId", envelope.operationId())
				.param("correlationId", envelope.correlationId())
				.param("sourceMessageId", envelope.sourceMessageId())
				.update();

		Optional<StoredQuarantine> stored = jdbcClient.sql("""
				SELECT quarantine_id, source_topic, source_partition, source_offset,
				       source_record_key, source_record_value, failure_code,
				       operation_id, correlation_id, source_message_id
				FROM quarantined_commands
				WHERE quarantine_id = :quarantineId
				""")
				.param("quarantineId", envelope.quarantineId())
				.query((resultSet, rowNumber) -> stored(resultSet))
				.optional();

		if (stored.isEmpty() || !stored.get().matches(envelope)) {
			throw new IllegalStateException("Conflicting immutable quarantine identity");
		}

		Counter.builder("paymentflow.dlq.entries")
				.tag("failure_code", envelope.failureCode())
				.register(meterRegistry)
				.increment();
	}

	private StoredQuarantine stored(ResultSet resultSet) throws SQLException {
		return new StoredQuarantine(
				resultSet.getObject("quarantine_id", UUID.class),
				resultSet.getString("source_topic"),
				resultSet.getInt("source_partition"),
				resultSet.getLong("source_offset"),
				resultSet.getBytes("source_record_key"),
				resultSet.getBytes("source_record_value"),
				resultSet.getString("failure_code"),
				resultSet.getObject("operation_id", UUID.class),
				resultSet.getObject("correlation_id", UUID.class),
				resultSet.getObject("source_message_id", UUID.class));
	}

	private record StoredQuarantine(
			UUID quarantineId,
			String sourceTopic,
			int sourcePartition,
			long sourceOffset,
			byte[] sourceRecordKey,
			byte[] sourceRecordValue,
			String failureCode,
			UUID operationId,
			UUID correlationId,
			UUID sourceMessageId) {

		boolean matches(PaymentOperationQuarantineEnvelopeDto envelope) {
			return quarantineId.equals(envelope.quarantineId())
					&& sourceTopic.equals(envelope.sourceTopic())
					&& sourcePartition == envelope.sourcePartition()
					&& sourceOffset == envelope.sourceOffset()
					&& Arrays.equals(sourceRecordKey, envelope.sourceRecordKey())
					&& Arrays.equals(sourceRecordValue, envelope.sourceRecordValue())
					&& failureCode.equals(envelope.failureCode())
					&& java.util.Objects.equals(operationId, envelope.operationId())
					&& java.util.Objects.equals(correlationId, envelope.correlationId())
					&& java.util.Objects.equals(sourceMessageId, envelope.sourceMessageId());
		}
	}
}
