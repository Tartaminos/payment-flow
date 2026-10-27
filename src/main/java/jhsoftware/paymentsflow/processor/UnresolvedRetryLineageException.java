package jhsoftware.paymentsflow.processor;

final class UnresolvedRetryLineageException extends RuntimeException {

	UnresolvedRetryLineageException() {
		super("Retry lineage is not durably known");
	}
}
