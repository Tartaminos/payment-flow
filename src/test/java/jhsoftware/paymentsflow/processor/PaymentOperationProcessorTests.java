package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
	void mismatchedKeyWithholdsAcknowledgmentAndNeverRunsAttempts() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(context("PENDING")));

		boolean ackEligible = processor(repository, executor).process(
				UUID.randomUUID().toString(), command());

		assertThat(ackEligible).isFalse();
		verify(executor, never()).execute(any(), any(), any());
	}

	@Test
	void unknownOperationWithholdsAcknowledgment() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.empty());

		boolean ackEligible = processor(repository, executor).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isFalse();
		verify(executor, never()).execute(any(), any(), any());
	}

	@Test
	void alreadyFinalizedOperationIsAcknowledgedWithoutRunningAttempts() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(context("SUCCEEDED")));

		boolean ackEligible = processor(repository, executor).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
		verify(executor, never()).execute(any(), any(), any());
	}

	@Test
	void unknownOperationWithOpenReconciliationJobAndHealthyScannerIsAcknowledgedWithoutRunningAttempts() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		ReconciliationScannerHealth scannerHealth = mock(ReconciliationScannerHealth.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(contextWithOpenJob()));
		when(scannerHealth.isOperational(NOW)).thenReturn(true);

		boolean ackEligible = processor(repository, executor, scannerHealth).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
		verify(executor, never()).execute(any(), any(), any());
	}

	@Test
	void unknownOperationWithOpenReconciliationJobAndUnhealthyScannerWithholdsAcknowledgment() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		ReconciliationScannerHealth scannerHealth = mock(ReconciliationScannerHealth.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(contextWithOpenJob()));
		when(scannerHealth.isOperational(NOW)).thenReturn(false);

		boolean ackEligible = processor(repository, executor, scannerHealth).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isFalse();
		verify(executor, never()).execute(any(), any(), any());
	}

	@Test
	void unsupportedOperationTypeWithholdsAcknowledgmentAndNeverRunsAnyExecutor() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		PaymentOperationCaptureAttemptExecutor captureExecutor = mock(PaymentOperationCaptureAttemptExecutor.class);
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(context("PENDING", "VOID")));

		boolean ackEligible = processor(repository, executor, captureExecutor).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isFalse();
		verify(executor, never()).execute(any(), any(), any());
		verify(captureExecutor, never()).execute(any(), any(), any());
	}

	@Test
	void pendingOperationDelegatesToAttemptExecutor() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		PaymentOperationContext pendingContext = context("PENDING");
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(pendingContext));
		when(executor.execute(pendingContext, CORRELATION_ID, null)).thenReturn(true);

		boolean ackEligible = processor(repository, executor).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
		verify(executor).execute(pendingContext, CORRELATION_ID, null);
	}

	@Test
	void pendingCaptureOperationDelegatesToCaptureAttemptExecutor() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		PaymentOperationCaptureAttemptExecutor captureExecutor = mock(PaymentOperationCaptureAttemptExecutor.class);
		PaymentOperationContext pendingContext = context("PENDING", "CAPTURE");
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(pendingContext));
		when(captureExecutor.execute(pendingContext, CORRELATION_ID, null)).thenReturn(true);

		boolean ackEligible = processor(repository, executor, captureExecutor).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
		verify(captureExecutor).execute(pendingContext, CORRELATION_ID, null);
		verify(executor, never()).execute(any(), any(), any());
	}

	@Test
	void freshlyCommittedUnknownWithHealthyScannerIsAcknowledged() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		ReconciliationScannerHealth scannerHealth = mock(ReconciliationScannerHealth.class);
		PaymentOperationContext pendingContext = context("PENDING");
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(pendingContext), Optional.of(context("UNKNOWN")));
		when(executor.execute(pendingContext, CORRELATION_ID, null)).thenReturn(true);
		when(scannerHealth.isOperational(NOW)).thenReturn(true);

		boolean ackEligible = processor(repository, executor, scannerHealth).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isTrue();
	}

	@Test
	void freshlyCommittedUnknownWithUnhealthyScannerWithholdsAcknowledgment() {
		PaymentOperationProcessingRepository repository = mock(PaymentOperationProcessingRepository.class);
		PaymentOperationAttemptExecutor executor = mock(PaymentOperationAttemptExecutor.class);
		ReconciliationScannerHealth scannerHealth = mock(ReconciliationScannerHealth.class);
		PaymentOperationContext pendingContext = context("PENDING");
		when(repository.loadContext(OPERATION_ID)).thenReturn(Optional.of(pendingContext), Optional.of(context("UNKNOWN")));
		when(executor.execute(pendingContext, CORRELATION_ID, null)).thenReturn(true);
		when(scannerHealth.isOperational(NOW)).thenReturn(false);

		boolean ackEligible = processor(repository, executor, scannerHealth).process(PAYMENT_ID.toString(), command());

		assertThat(ackEligible).isFalse();
	}

	private PaymentOperationProcessor processor(
			PaymentOperationProcessingRepository repository, PaymentOperationAttemptExecutor executor) {
		return processor(repository, executor, mock(PaymentOperationCaptureAttemptExecutor.class), mock(ReconciliationScannerHealth.class));
	}

	private PaymentOperationProcessor processor(
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor executor,
			PaymentOperationCaptureAttemptExecutor captureExecutor) {
		return processor(repository, executor, captureExecutor, mock(ReconciliationScannerHealth.class));
	}

	private PaymentOperationProcessor processor(
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor executor,
			ReconciliationScannerHealth scannerHealth) {
		return processor(repository, executor, mock(PaymentOperationCaptureAttemptExecutor.class), scannerHealth);
	}

	private PaymentOperationProcessor processor(
			PaymentOperationProcessingRepository repository,
			PaymentOperationAttemptExecutor executor,
			PaymentOperationCaptureAttemptExecutor captureExecutor,
			ReconciliationScannerHealth scannerHealth) {
		return new PaymentOperationProcessor(
				new PaymentOperationCommandDecoder(), repository, executor, captureExecutor, scannerHealth,
				Clock.fixed(NOW, ZoneOffset.UTC));
	}

	private PaymentOperationContext context(String operationStatus) {
		return context(operationStatus, "AUTHORIZE");
	}

	private PaymentOperationContext context(String operationStatus, String operationType) {
		return new PaymentOperationContext(
				OPERATION_ID, operationType, operationStatus, 50000L, "BRL", PAYMENT_ID, UUID.randomUUID(), "CREATED",
				0, null, false, null);
	}

	private PaymentOperationContext contextWithOpenJob() {
		return new PaymentOperationContext(
				OPERATION_ID, "AUTHORIZE", "UNKNOWN", 50000L, "BRL", PAYMENT_ID, UUID.randomUUID(), "CREATED",
				3, null, true, null);
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
