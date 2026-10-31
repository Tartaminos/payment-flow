package jhsoftware.paymentsflow.payment.command;

import java.util.UUID;

public record CaptureAdmissionCommand(UUID paymentId, long amountMinor, String currency, String idempotencyKey) {
}
