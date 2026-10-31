package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.dto.PaymentOperationRetryEnvelopeDto;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;
import jhsoftware.paymentsflow.processor.service.TechnicalFailureClassifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class PaymentOperationRetryEnvelopeDecoderTests {

	private final PaymentOperationRetryEnvelopeDecoder decoder = new PaymentOperationRetryEnvelopeDecoder();

	@Test
	void decodesAndRecoversMissingIdentitiesFromTheOriginalCommand() throws Exception {
		GenericRecord record = validRecord();
		PaymentOperationRetryEnvelopeDto decoded = decoder.decode(record);
		PaymentOperationCommand original = new PaymentOperationCommand(
				UUID.randomUUID(), "ExecutePaymentOperation", UUID.randomUUID(), UUID.randomUUID(), null, Instant.now());

		PaymentOperationRetryEnvelopeDto normalized = decoder.validateOriginal(decoded, original);

		assertThat(normalized.operationId()).isEqualTo(original.operationId());
		assertThat(normalized.correlationId()).isEqualTo(original.correlationId());
		assertThat(normalized.sourceMessageId()).isEqualTo(original.messageId());
	}

	@Test
	void rejectsRetryIdentityThatDoesNotMatchSourceCoordinates() throws Exception {
		GenericRecord record = validRecord();
		record.put("retryId", UUID.randomUUID().toString());

		assertThatThrownBy(() -> decoder.decode(record))
				.isInstanceOf(PoisonCommandException.class)
				.extracting(exception -> ((PoisonCommandException) exception).failureCode())
				.isEqualTo(PoisonFailureCode.INVALID_COMMAND_PAYLOAD);
	}

	@Test
	void rejectsEnvelopeIdentityThatConflictsWithOriginalCommand() throws Exception {
		GenericRecord record = validRecord();
		UUID envelopeOperationId = UUID.randomUUID();
		record.put("operationId", envelopeOperationId.toString());
		PaymentOperationRetryEnvelopeDto decoded = decoder.decode(record);
		PaymentOperationCommand original = new PaymentOperationCommand(
				UUID.randomUUID(), "ExecutePaymentOperation", UUID.randomUUID(), UUID.randomUUID(), null, Instant.now());

		assertThatThrownBy(() -> decoder.validateOriginal(decoded, original))
				.isInstanceOf(PoisonCommandException.class);
	}

	private GenericRecord validRecord() throws Exception {
		Schema schema;
		try (InputStream input = getClass().getResourceAsStream(
				"/avro/com/paymentflow/retry/PaymentOperationRetry.avsc")) {
			schema = new Schema.Parser().parse(input);
		}
		String sourceTopic = "payment.operations.requested.v1";
		int sourcePartition = 2;
		long sourceOffset = 19L;
		GenericRecord record = new GenericData.Record(schema);
		record.put("retryId", PaymentOperationRetryIdentity.retryId(
				sourceTopic, sourcePartition, sourceOffset).toString());
		record.put("sourceTopic", sourceTopic);
		record.put("sourcePartition", sourcePartition);
		record.put("sourceOffset", sourceOffset);
		record.put("sourceRecordKey", ByteBuffer.wrap(new byte[] {1}));
		record.put("sourceRecordValue", ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 1}));
		record.put("failureClass", TechnicalFailureClassifier.DATA_ACCESS_UNAVAILABLE);
		record.put("failedAt", Instant.parse("2026-10-27T10:00:00Z").toEpochMilli());
		record.put("operationId", null);
		record.put("correlationId", null);
		record.put("sourceMessageId", null);
		return record;
	}
}
