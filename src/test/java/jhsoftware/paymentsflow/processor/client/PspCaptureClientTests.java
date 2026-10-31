package jhsoftware.paymentsflow.processor.client;

import jhsoftware.paymentsflow.processor.model.PaymentOperationContext;
import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcome;
import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcomeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServiceUnavailable;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class PspCaptureClientTests {

	private static final UUID AUTHORIZATION_PSP_OPERATION_ID = UUID.randomUUID();
	private static final PaymentOperationContext CONTEXT = new PaymentOperationContext(
			UUID.randomUUID(), "CAPTURE", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "AUTHORIZED",
			0, null, false, AUTHORIZATION_PSP_OPERATION_ID.toString());

	@Test
	void activeValidityAndSucceededResponseYieldsSucceededOutcomeWithPspReference() {
		UUID pspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + AUTHORIZATION_PSP_OPERATION_ID))
				.andExpect(method(HttpMethod.GET))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":"ACTIVE","validityEvidenceAt":"2026-10-25T10:00:00Z"}
						""".formatted(AUTHORIZATION_PSP_OPERATION_ID),
						MediaType.APPLICATION_JSON));
		server.expect(requestTo("/simulator/operations/capture"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"SUCCEEDED\",\"reasonCode\":null}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspCaptureClient(builder.build()).capture(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.SUCCEEDED);
		assertThat(outcome.pspReference()).isEqualTo(pspOperationId.toString());
		server.verify();
	}

	@Test
	void expiredValidityRejectsWithoutIssuingTheCapturePost() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + AUTHORIZATION_PSP_OPERATION_ID))
				.andExpect(method(HttpMethod.GET))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":"EXPIRED","validityEvidenceAt":"2026-10-25T10:00:00Z"}
						""".formatted(AUTHORIZATION_PSP_OPERATION_ID),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspCaptureClient(builder.build()).capture(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.REJECTED);
		assertThat(outcome.reasonCode()).isEqualTo("AUTHORIZATION_EXPIRED");
		server.verify();
	}

	@Test
	void ambiguousValidityLookupYieldsUnknownOutcomeWithoutIssuingTheCapturePost() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + AUTHORIZATION_PSP_OPERATION_ID))
				.andExpect(method(HttpMethod.GET))
				.andRespond(withServiceUnavailable());

		PspAuthorizationOutcome outcome = new PspCaptureClient(builder.build()).capture(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		server.verify();
	}

	@Test
	void unreachableValidityLookupYieldsUnknownOutcome() {
		RestClient restClient = RestClient.builder().baseUrl("http://127.0.0.1:1").build();

		PspAuthorizationOutcome outcome = new PspCaptureClient(restClient).capture(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		assertThat(outcome.reasonCode()).isEqualTo("PSP_VALIDITY_LOOKUP_UNREACHABLE");
	}

	@Test
	void rejectedCaptureResponseYieldsRejectedOutcomeWithReasonCode() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + AUTHORIZATION_PSP_OPERATION_ID))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":"ACTIVE","validityEvidenceAt":"2026-10-25T10:00:00Z"}
						""".formatted(AUTHORIZATION_PSP_OPERATION_ID),
						MediaType.APPLICATION_JSON));
		UUID pspOperationId = UUID.randomUUID();
		server.expect(requestTo("/simulator/operations/capture"))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"REJECTED\",\"reasonCode\":\"NON_EXECUTION_CONFIRMED\"}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspCaptureClient(builder.build()).capture(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.REJECTED);
		assertThat(outcome.reasonCode()).isEqualTo("NON_EXECUTION_CONFIRMED");
	}
}
