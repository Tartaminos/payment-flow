package jhsoftware.paymentsflow.processor.dto;

import java.time.Instant;
import java.util.UUID;

public record PaymentOperationRetryEnvelopeDto(
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
