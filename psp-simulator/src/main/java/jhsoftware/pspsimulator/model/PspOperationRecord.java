package jhsoftware.pspsimulator.model;

import jhsoftware.pspsimulator.dto.AuthorizeOperationRequestDto;
import jhsoftware.pspsimulator.dto.CaptureOperationRequestDto;
import jhsoftware.pspsimulator.dto.VoidOperationRequestDto;

import java.time.Instant;
import java.util.UUID;

public record PspOperationRecord(
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

	public boolean matches(AuthorizeOperationRequestDto request) {
		return paymentId.equals(request.paymentId())
				&& amountMinor == request.amountMinor()
				&& currency.equals(request.currency());
	}

	public boolean matches(CaptureOperationRequestDto request) {
		return paymentId.equals(request.paymentId())
				&& amountMinor == request.amountMinor()
				&& currency.equals(request.currency())
				&& "CAPTURE".equals(operationType)
				&& authorizationPspOperationId != null
				&& authorizationPspOperationId.equals(request.authorizationPspOperationId());
	}

	public boolean matches(VoidOperationRequestDto request) {
		return paymentId.equals(request.paymentId())
				&& amountMinor == request.amountMinor()
				&& currency.equals(request.currency())
				&& "VOID".equals(operationType)
				&& authorizationPspOperationId != null
				&& authorizationPspOperationId.equals(request.authorizationPspOperationId());
	}
}
