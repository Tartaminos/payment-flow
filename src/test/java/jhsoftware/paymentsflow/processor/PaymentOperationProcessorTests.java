package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class PaymentOperationProcessorTests {

	private static final UUID OPERATION_ID = UUID.fromString("22222222-2222-4222-8222-222222222222");
	private static final UUID PAYMENT_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
	private static final UUID CORRELATION_ID = UUID.fromString("33333333-3333-4333-8333-333333333333");
	private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

	@Test
	void mismatchedKeyWithholdsAcknowledgmentAndNeverCallsPsp() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PspAuthorizationClient pspClient = mock(PspAuthorizationClient.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(context("PENDING")));

		boolean ackEligible = processor(repository, pspClient).process(
				UUID.randomUUID().toString(), command());

		assertThat(ackEligible).isFalse();
		verify(pspClient, never()).authorize(any());
		verify(repository, never()).commitOutcome(any(), any(), any(), any(), any());
	}

	@Test
	void unknownOperationWithholdsAcknowledgment() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PspAuthorizationClient pspClient = mock(PspAuthorizationClient.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.empty());

		boolean ackEligible = processor(repository, pspClient).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isFalse();
		verify(pspClient, never()).authorize(any());
	}

	@Test
	void alreadyFinalizedOperationIsAcknowledgedWithoutCallingPsp() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PspAuthorizationClient pspClient = mock(PspAuthorizationClient.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(context("SUCCEEDED")));

		boolean ackEligible = processor(repository, pspClient).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
		verify(pspClient, never()).authorize(any());
	}

	@Test
	void nonAuthorizeOperationTypeWithholdsAcknowledgmentAndNeverCallsPsp() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PspAuthorizationClient pspClient = mock(PspAuthorizationClient.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(context("PENDING", "CAPTURE")));

		boolean ackEligible = processor(repository, pspClient).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isFalse();
		verify(pspClient, never()).authorize(any());
		verify(repository, never()).commitOutcome(any(), any(), any(), any(), any());
	}

	@Test
	void pendingOperationCallsPspOutsideTransactionThenCommits() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PspAuthorizationClient pspClient = mock(PspAuthorizationClient.class);
		PaymentOperationContext pendingContext = context("PENDING");
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(pendingContext));
		PspAuthorizationOutcome outcome = PspAuthorizationOutcome.succeeded(OPERATION_ID.toString());
		when(pspClient.authorize(pendingContext)).thenReturn(outcome);
		when(repository.commitOutcome(eq(OPERATION_ID), eq(outcome), eq(CORRELATION_ID), any(), eq(NOW)))
				.thenReturn(true);

		boolean ackEligible = processor(repository, pspClient).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
		verify(pspClient).authorize(pendingContext);
		verify(repository).commitOutcome(OPERATION_ID, outcome, CORRELATION_ID, null, NOW);
	}

	private PaymentOperationProcessor processor(
			PaymentOperationProcessingRepository repository, PspAuthorizationClient pspClient) {
		return new PaymentOperationProcessor(
				new PaymentOperationCommandDecoder(),
				repository,
				pspClient,
				Clock.fixed(NOW, ZoneOffset.UTC));
	}

	private PaymentOperationContext context(String operationStatus) {
		return context(operationStatus, "AUTHORIZE");
	}

	private PaymentOperationContext context(String operationStatus, String operationType) {
		return new PaymentOperationContext(
				OPERATION_ID, operationType, operationStatus, 50000L, "BRL", PAYMENT_ID, UUID.randomUUID(), "CREATED");
	}

	private GenericRecord command() {
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
		record.put("messageId", UUID.randomUUID().toString());
		record.put("commandType", "ExecutePaymentOperation");
		record.put("operationId", OPERATION_ID.toString());
		record.put("correlationId", CORRELATION_ID.toString());
		record.put("occurredAt", NOW.toEpochMilli());
		record.put("causationId", null);
		return record;
	}
}
