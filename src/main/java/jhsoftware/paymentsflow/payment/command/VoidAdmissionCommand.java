package jhsoftware.paymentsflow.payment.command;

import java.util.UUID;

public record VoidAdmissionCommand(UUID paymentId, long amountMinor, String currency, String idempotencyKey) {
}
