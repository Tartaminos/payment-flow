package jhsoftware.paymentsflow.payment;

import java.util.UUID;

record VoidAdmissionCommand(UUID paymentId, long amountMinor, String currency, String idempotencyKey) {
}
