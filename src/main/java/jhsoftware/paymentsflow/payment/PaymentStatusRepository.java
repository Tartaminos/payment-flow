package jhsoftware.paymentsflow.payment;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class PaymentStatusRepository {

	private final JdbcClient jdbcClient;

	PaymentStatusRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	Optional<PaymentStatusResponse> findStatus(UUID merchantId, UUID paymentId) {
		return jdbcClient.sql("""
				SELECT p.id AS payment_id,
				       p.external_order_id AS order_id,
				       p.amount_minor AS amount_minor,
				       p.currency AS currency,
				       p.status AS payment_status,
				       po.id AS operation_id,
				       po.type AS operation_type,
				       po.status AS operation_status
				FROM payments p
				LEFT JOIN LATERAL (
				    SELECT id, type, status
				    FROM payment_operations
				    WHERE payment_id = p.id
				    ORDER BY created_at DESC, id DESC
				    LIMIT 1
				) po ON true
				WHERE p.id = :paymentId
				  AND p.merchant_id = :merchantId
				""")
				.param("paymentId", paymentId)
				.param("merchantId", merchantId)
				.query(this::toResponse)
				.optional();
	}

	private PaymentStatusResponse toResponse(ResultSet resultSet, int rowNumber) throws SQLException {
		UUID operationId = resultSet.getObject("operation_id", UUID.class);
		PaymentStatusResponse.LatestOperation latestOperation = operationId == null
				? null
				: new PaymentStatusResponse.LatestOperation(
						operationId,
						resultSet.getString("operation_type"),
						resultSet.getString("operation_status"));

		return new PaymentStatusResponse(
				resultSet.getObject("payment_id", UUID.class),
				resultSet.getString("order_id"),
				resultSet.getLong("amount_minor"),
				resultSet.getString("currency"),
				resultSet.getString("payment_status"),
				latestOperation);
	}
}
