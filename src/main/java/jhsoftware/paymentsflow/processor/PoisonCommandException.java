package jhsoftware.paymentsflow.processor;

final class PoisonCommandException extends RuntimeException {

	private final PoisonFailureCode failureCode;

	PoisonCommandException(PoisonFailureCode failureCode) {
		super(failureCode.name());
		this.failureCode = failureCode;
	}

	PoisonCommandException(PoisonFailureCode failureCode, Throwable cause) {
		super(failureCode.name(), cause);
		this.failureCode = failureCode;
	}

	PoisonFailureCode failureCode() {
		return failureCode;
	}
}
