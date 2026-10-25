package jhsoftware.pspsimulator;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

record AuthorizeOperationRequest(
		@NotNull UUID pspOperationId,
		@NotNull UUID merchantId,
		@NotNull UUID paymentId,
		@Positive long amountMinor,
		@NotNull @Pattern(regexp = "^[A-Z]{3}$") String currency) {
}
