package jhsoftware.paymentsflow.payment.dto;

import jakarta.validation.constraints.NotNull;
import tools.jackson.databind.JsonNode;

import jhsoftware.paymentsflow.payment.command.PaymentAdmissionCommand;
import jhsoftware.paymentsflow.payment.exception.InvalidPaymentRequestException;

public record CreatePaymentRequestDto(
		@NotNull JsonNode orderId,
		@NotNull JsonNode amount,
		@NotNull JsonNode currency) {

	public PaymentAdmissionCommand toCommand(String idempotencyKey) {
		if (!orderId.isTextual() || !currency.isTextual()) {
			throw new InvalidPaymentRequestException();
		}
		if (!amount.isIntegralNumber() || !amount.canConvertToLong()) {
			throw new InvalidPaymentRequestException();
		}

		String externalOrderId = orderId.textValue();
		String currencyCode = currency.textValue();
		long amountMinor = amount.longValue();
		if (externalOrderId.isBlank()
				|| externalOrderId.codePointCount(0, externalOrderId.length()) > 100
				|| !externalOrderId.equals(externalOrderId.strip())
				|| !currencyCode.matches("[A-Z]{3}")
				|| amountMinor <= 0) {
			throw new InvalidPaymentRequestException();
		}

		return new PaymentAdmissionCommand(externalOrderId, amountMinor, currencyCode, idempotencyKey);
	}
}
