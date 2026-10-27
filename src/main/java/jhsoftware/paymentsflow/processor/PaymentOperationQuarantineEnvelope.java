package jhsoftware.paymentsflow.processor;

import java.time.Instant;
import java.util.UUID;

record PaymentOperationQuarantineEnvelope(
		UUID quarantineId,
		String sourceTopic,
		int sourcePartition,
		long sourceOffset,
		byte[] sourceRecordKey,
		byte[] sourceRecordValue,
		String failureCode,
		Instant quarantinedAt,
		UUID operationId,
		UUID correlationId,
		UUID sourceMessageId) {

	PaymentOperationQuarantineEnvelope {
		sourceRecordKey = sourceRecordKey.clone();
		sourceRecordValue = sourceRecordValue.clone();
	}

	@Override
	public byte[] sourceRecordKey() {
		return sourceRecordKey.clone();
	}

	@Override
	public byte[] sourceRecordValue() {
		return sourceRecordValue.clone();
	}
}
