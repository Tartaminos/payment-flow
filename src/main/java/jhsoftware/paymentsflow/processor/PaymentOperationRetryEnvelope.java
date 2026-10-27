package jhsoftware.paymentsflow.processor;

import java.time.Instant;
import java.util.UUID;

record PaymentOperationRetryEnvelope(
		UUID retryId,
		String sourceTopic,
		int sourcePartition,
		long sourceOffset,
		byte[] sourceRecordKey,
		byte[] sourceRecordValue,
		String failureClass,
		Instant failedAt,
		UUID operationId,
		UUID correlationId,
		UUID sourceMessageId) {
}
