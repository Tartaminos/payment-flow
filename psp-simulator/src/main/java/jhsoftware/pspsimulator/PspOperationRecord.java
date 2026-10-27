package jhsoftware.pspsimulator;

import java.time.Instant;
import java.util.UUID;

record PspOperationRecord(
		UUID id,
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

	boolean matches(AuthorizeOperationRequest request) {
		return paymentId.equals(request.paymentId())
				&& amountMinor == request.amountMinor()
				&& currency.equals(request.currency());
	}

	boolean matches(CaptureOperationRequest request) {
		return paymentId.equals(request.paymentId())
				&& amountMinor == request.amountMinor()
				&& currency.equals(request.currency())
				&& "CAPTURE".equals(operationType)
				&& authorizationPspOperationId != null
				&& authorizationPspOperationId.equals(request.authorizationPspOperationId());
	}

	boolean matches(VoidOperationRequest request) {
		return paymentId.equals(request.paymentId())
				&& amountMinor == request.amountMinor()
				&& currency.equals(request.currency())
				&& "VOID".equals(operationType)
				&& authorizationPspOperationId != null
				&& authorizationPspOperationId.equals(request.authorizationPspOperationId());
	}
}
