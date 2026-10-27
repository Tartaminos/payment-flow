package jhsoftware.paymentsflow.payment;

import java.util.UUID;

record CaptureAdmissionCommand(UUID paymentId, long amountMinor, String currency, String idempotencyKey) {
}
