package jhsoftware.paymentsflow.processor;

import java.util.UUID;

record PaymentOperationContext(
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

	boolean isFinalized() {
		return operationStatus.equals("SUCCEEDED")
				|| operationStatus.equals("REJECTED")
				|| operationStatus.equals("FAILED");
	}

	boolean isUnknownAwaitingReconciliation() {
		return operationStatus.equals("UNKNOWN") && hasUnresolvedReconciliationJob;
	}
}
