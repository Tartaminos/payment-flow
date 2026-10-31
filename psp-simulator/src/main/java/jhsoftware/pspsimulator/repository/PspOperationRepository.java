package jhsoftware.pspsimulator.repository;

import jhsoftware.pspsimulator.model.PspOperationRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PspOperationRepository {

	private final JdbcClient jdbcClient;

	PspOperationRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	public Optional<PspOperationRecord> findByMerchantAndPspOperationId(UUID merchantId, UUID pspOperationId) {
		return jdbcClient.sql("""
				SELECT id, merchant_id, psp_operation_id, payment_id, amount_minor, currency, decision, decision_reason,
				       operation_type, authorization_psp_operation_id, validity, validity_evidence_at, created_at
				FROM psp_operations
				WHERE merchant_id = :merchantId AND psp_operation_id = :pspOperationId
				""")
				.param("merchantId", merchantId)
				.param("pspOperationId", pspOperationId)
				.query(this::toRecord)
				.optional();
	}

	public Optional<PspOperationRecord> findByPspOperationId(UUID pspOperationId) {
		return jdbcClient.sql("""
				SELECT id, merchant_id, psp_operation_id, payment_id, amount_minor, currency, decision, decision_reason,
				       operation_type, authorization_psp_operation_id, validity, validity_evidence_at, created_at
				FROM psp_operations
				WHERE psp_operation_id = :pspOperationId
				""")
				.param("pspOperationId", pspOperationId)
				.query(this::toRecord)
				.optional();
	}

	public void insert(
			UUID merchantId,
			UUID pspOperationId,
			UUID paymentId,
			long amountMinor,
			String currency,
			String decision,
			String decisionReason,
			String operationType,
			UUID authorizationPspOperationId,
			String validity,
			Instant validityEvidenceAt,
			Instant createdAt) {
		jdbcClient.sql("""
				INSERT INTO psp_operations
				    (id, merchant_id, psp_operation_id, payment_id, amount_minor, currency, decision, decision_reason,
				     operation_type, authorization_psp_operation_id, validity, validity_evidence_at, created_at)
				VALUES
				    (:id, :merchantId, :pspOperationId, :paymentId, :amountMinor, :currency, :decision, :decisionReason,
				     :operationType, :authorizationPspOperationId, :validity, :validityEvidenceAt, :createdAt)
				""")
				.param("id", UUID.randomUUID())
				.param("merchantId", merchantId)
				.param("pspOperationId", pspOperationId)
				.param("paymentId", paymentId)
				.param("amountMinor", amountMinor)
				.param("currency", currency)
				.param("decision", decision)
				.param("decisionReason", decisionReason)
				.param("operationType", operationType)
				.param("authorizationPspOperationId", authorizationPspOperationId)
				.param("validity", validity)
				.param("validityEvidenceAt", toDatabaseTimestamp(validityEvidenceAt))
				.param("createdAt", createdAt.atOffset(ZoneOffset.UTC))
				.update();
	}

	private PspOperationRecord toRecord(ResultSet resultSet, int rowNumber) throws SQLException {
		OffsetDateTime validityEvidenceAt = resultSet.getObject("validity_evidence_at", OffsetDateTime.class);
		return new PspOperationRecord(
				resultSet.getObject("id", UUID.class),
				resultSet.getObject("merchant_id", UUID.class),
				resultSet.getObject("psp_operation_id", UUID.class),
				resultSet.getObject("payment_id", UUID.class),
				resultSet.getLong("amount_minor"),
				resultSet.getString("currency"),
				resultSet.getString("decision"),
				resultSet.getString("decision_reason"),
				resultSet.getString("operation_type"),
				resultSet.getObject("authorization_psp_operation_id", UUID.class),
				resultSet.getString("validity"),
				validityEvidenceAt == null ? null : validityEvidenceAt.toInstant(),
				resultSet.getObject("created_at", OffsetDateTime.class).toInstant());
	}

	private OffsetDateTime toDatabaseTimestamp(Instant instant) {
		return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
	}
}
