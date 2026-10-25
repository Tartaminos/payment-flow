package jhsoftware.paymentsflow.payment;

record PaymentAdmissionCommand(String orderId, long amountMinor, String currency, String idempotencyKey) {
}
