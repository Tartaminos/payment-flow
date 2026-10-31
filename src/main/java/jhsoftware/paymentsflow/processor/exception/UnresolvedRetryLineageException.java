package jhsoftware.paymentsflow.processor.exception;

public final class UnresolvedRetryLineageException extends RuntimeException {

	public UnresolvedRetryLineageException() {
		super("Retry lineage is not durably known");
	}
}
