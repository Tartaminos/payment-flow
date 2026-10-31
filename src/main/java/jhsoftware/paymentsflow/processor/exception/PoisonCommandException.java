package jhsoftware.paymentsflow.processor.exception;

public final class PoisonCommandException extends RuntimeException {

	private final PoisonFailureCode failureCode;

	public PoisonCommandException(PoisonFailureCode failureCode) {
		super(failureCode.name());
		this.failureCode = failureCode;
	}

	public PoisonCommandException(PoisonFailureCode failureCode, Throwable cause) {
		super(failureCode.name(), cause);
		this.failureCode = failureCode;
	}

	public PoisonFailureCode failureCode() {
		return failureCode;
	}
}
