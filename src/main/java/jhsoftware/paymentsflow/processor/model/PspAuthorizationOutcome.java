package jhsoftware.paymentsflow.processor.model;

public record PspAuthorizationOutcome(PspAuthorizationOutcomeType type, String pspReference, String reasonCode) {

	public static PspAuthorizationOutcome succeeded(String pspReference) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.SUCCEEDED, pspReference, null);
	}

	public static PspAuthorizationOutcome rejected(String pspReference, String reasonCode) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.REJECTED, pspReference, reasonCode);
	}

	public static PspAuthorizationOutcome failed(String pspReference, String reasonCode) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.FAILED, pspReference, reasonCode);
	}

	public static PspAuthorizationOutcome unknown(String reasonCode) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.UNKNOWN, null, reasonCode);
	}

	public boolean isDefinitive() {
		return type != PspAuthorizationOutcomeType.UNKNOWN;
	}
}
