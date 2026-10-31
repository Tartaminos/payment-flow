package jhsoftware.paymentsflow.payment.command;

public record PaymentAdmissionCommand(String orderId, long amountMinor, String currency, String idempotencyKey) {
}
