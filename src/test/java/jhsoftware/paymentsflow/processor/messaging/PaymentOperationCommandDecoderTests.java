package jhsoftware.paymentsflow.processor.messaging;

import jhsoftware.paymentsflow.processor.command.PaymentOperationCommand;
import jhsoftware.paymentsflow.processor.exception.PoisonCommandException;
import jhsoftware.paymentsflow.processor.exception.PoisonFailureCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class PaymentOperationCommandDecoderTests {

	@Test
	void diagnosticDecodePreservesValidIdentitiesFromUnsupportedCommandType() {
		UUID messageId = UUID.randomUUID();
		UUID operationId = UUID.randomUUID();
		UUID correlationId = UUID.randomUUID();
		GenericRecord record = command(messageId, operationId, correlationId, "UnsupportedCommand");
		PaymentOperationCommandDecoder decoder = new PaymentOperationCommandDecoder();

		assertThatThrownBy(() -> decoder.decode(record))
				.isInstanceOfSatisfying(PoisonCommandException.class,
						exception -> assertThat(exception.failureCode())
								.isEqualTo(PoisonFailureCode.UNSUPPORTED_COMMAND_TYPE));
		PaymentOperationCommand diagnostic = decoder.decodeForDiagnostics(record);
		assertThat(diagnostic.messageId()).isEqualTo(messageId);
		assertThat(diagnostic.operationId()).isEqualTo(operationId);
		assertThat(diagnostic.correlationId()).isEqualTo(correlationId);
	}

	private GenericRecord command(UUID messageId, UUID operationId, UUID correlationId, String commandType) {
		Schema schema = SchemaBuilder.record("ExecutePaymentOperation")
				.namespace("com.paymentflow.commands")
				.fields()
				.name("messageId").type().stringType().noDefault()
				.name("commandType").type().stringType().noDefault()
				.name("operationId").type().stringType().noDefault()
				.name("correlationId").type().stringType().noDefault()
				.name("occurredAt").type().longType().noDefault()
				.name("causationId").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
				.endRecord();
		GenericRecord record = new GenericData.Record(schema);
		record.put("messageId", messageId.toString());
		record.put("commandType", commandType);
		record.put("operationId", operationId.toString());
		record.put("correlationId", correlationId.toString());
		record.put("occurredAt", Instant.parse("2026-10-27T10:00:00Z").toEpochMilli());
		record.put("causationId", null);
		return record;
	}
}
