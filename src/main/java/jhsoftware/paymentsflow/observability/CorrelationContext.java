package jhsoftware.paymentsflow.observability;

import org.slf4j.MDC;

public final class CorrelationContext {

	public static final String CORRELATION_ID = "correlationId";
	public static final String PAYMENT_ID = "paymentId";
	public static final String OPERATION_ID = "operationId";
	public static final String MESSAGE_ID = "messageId";
	public static final String RETRY_ID = "retryId";

	private CorrelationContext() {
	}

	public static void put(String key, String value) {
		if (value == null) {
			return;
		}
		MDC.put(key, value);
	}

	public static void put(String key, Object value) {
		if (value == null) {
			return;
		}
		MDC.put(key, value.toString());
	}

	public static void clear() {
		MDC.clear();
	}
}
