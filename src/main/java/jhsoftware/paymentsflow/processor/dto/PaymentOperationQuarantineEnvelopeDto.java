package jhsoftware.paymentsflow.processor.dto;

import java.time.Instant;
import java.util.UUID;

public record PaymentOperationQuarantineEnvelopeDto(
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

	public PaymentOperationQuarantineEnvelopeDto {
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
