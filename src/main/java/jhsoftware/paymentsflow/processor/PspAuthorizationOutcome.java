package jhsoftware.paymentsflow.processor;

record PspAuthorizationOutcome(PspAuthorizationOutcomeType type, String pspReference, String reasonCode) {

	static PspAuthorizationOutcome succeeded(String pspReference) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.SUCCEEDED, pspReference, null);
	}

	static PspAuthorizationOutcome rejected(String pspReference, String reasonCode) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.REJECTED, pspReference, reasonCode);
	}

	static PspAuthorizationOutcome failed(String pspReference, String reasonCode) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.FAILED, pspReference, reasonCode);
	}

	static PspAuthorizationOutcome unknown(String reasonCode) {
		return new PspAuthorizationOutcome(PspAuthorizationOutcomeType.UNKNOWN, null, reasonCode);
	}

	boolean isDefinitive() {
		return type != PspAuthorizationOutcomeType.UNKNOWN;
	}
}
