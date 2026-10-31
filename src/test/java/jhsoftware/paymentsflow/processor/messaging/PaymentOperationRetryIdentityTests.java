package jhsoftware.paymentsflow.processor.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.Test;

class PaymentOperationRetryIdentityTests {

	@Test
	void sameSourceCoordinatesYieldIdenticalRetryId() {
		UUID first = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 2, 17L);
		UUID second = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 2, 17L);

		assertThat(first).isEqualTo(second);
	}

	@Test
	void differentSourceCoordinatesYieldDifferentRetryId() {
		UUID byOffset = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 2, 17L);
		UUID byDifferentOffset = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 2, 18L);
		UUID byDifferentPartition = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 3, 17L);
		UUID byDifferentTopic = PaymentOperationRetryIdentity.retryId("payment.operations.retry.v1", 2, 17L);

		assertThat(byOffset).isNotEqualTo(byDifferentOffset);
		assertThat(byOffset).isNotEqualTo(byDifferentPartition);
		assertThat(byOffset).isNotEqualTo(byDifferentTopic);
	}

	@Test
	void retryIdHasVersionFiveAndRfc4122VariantBits() {
		UUID retryId = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 0, 0L);

		assertThat(retryId.version()).isEqualTo(5);
		assertThat(retryId.variant()).isEqualTo(2);
	}

	@Test
	void retryIdMatchesIndependentlyComputedRfc4122UuidV5Vector() {
		UUID retryId = PaymentOperationRetryIdentity.retryId("payment.operations.requested.v1", 2, 17L);

		assertThat(retryId.toString()).isEqualTo("4f6c7030-a668-5a24-b8a9-a2ddadd44857");
	}

	@Test
	void quarantineIdIsStableVersionFiveAndDistinctFromRetryIdentity() {
		UUID quarantineId = PaymentOperationRetryIdentity.quarantineId(
				"payment.operations.requested.v1", 2, 17L);

		assertThat(quarantineId).isEqualTo(PaymentOperationRetryIdentity.quarantineId(
				"payment.operations.requested.v1", 2, 17L));
		assertThat(quarantineId.version()).isEqualTo(5);
		assertThat(quarantineId.variant()).isEqualTo(2);
		assertThat(quarantineId).isNotEqualTo(PaymentOperationRetryIdentity.retryId(
				"payment.operations.requested.v1", 2, 17L));
	}
}
