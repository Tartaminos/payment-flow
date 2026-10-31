package jhsoftware.paymentsflow.processor.model;

import java.util.UUID;

public record PaymentOperationContext(
		UUID operationId,
		String operationType,
		String operationStatus,
		long amountMinor,
		String currency,
		UUID paymentId,
		UUID merchantId,
		String paymentStatus,
		int attemptCount,
		UUID leaseOwner,
		boolean hasUnresolvedReconciliationJob,
		String authorizationPspReference) {

	public boolean isFinalized() {
		return operationStatus.equals("SUCCEEDED")
				|| operationStatus.equals("REJECTED")
				|| operationStatus.equals("FAILED");
	}

	public boolean isUnknownAwaitingReconciliation() {
		return operationStatus.equals("UNKNOWN") && hasUnresolvedReconciliationJob;
	}
}
