package jhsoftware.paymentsflow.processor.client;

import jhsoftware.paymentsflow.processor.model.PaymentOperationContext;
import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcome;
import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcomeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class PspVoidClientTests {

	private static final UUID AUTHORIZATION_PSP_OPERATION_ID = UUID.randomUUID();
	private static final PaymentOperationContext CONTEXT = new PaymentOperationContext(
			UUID.randomUUID(), "VOID", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "AUTHORIZED",
			0, null, false, AUTHORIZATION_PSP_OPERATION_ID.toString());

	@Test
	void succeededResponseYieldsSucceededOutcomeWithPspReferenceWithoutAnyPriorValidityLookup() {
		UUID pspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/void"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"SUCCEEDED\",\"reasonCode\":null}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspVoidClient(builder.build()).voidOperation(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.SUCCEEDED);
		assertThat(outcome.pspReference()).isEqualTo(pspOperationId.toString());
		server.verify();
	}

	@Test
	void rejectedVoidResponseYieldsRejectedOutcomeWithReasonCode() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		UUID pspOperationId = UUID.randomUUID();
		server.expect(requestTo("/simulator/operations/void"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"REJECTED\",\"reasonCode\":\"AUTHORIZATION_EXPIRED\"}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspVoidClient(builder.build()).voidOperation(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.REJECTED);
		assertThat(outcome.reasonCode()).isEqualTo("AUTHORIZATION_EXPIRED");
		server.verify();
	}

	@Test
	void unreachablePspYieldsUnknownOutcome() {
		RestClient restClient = RestClient.builder().baseUrl("http://127.0.0.1:1").build();

		PspAuthorizationOutcome outcome = new PspVoidClient(restClient).voidOperation(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		assertThat(outcome.reasonCode()).isEqualTo("PSP_UNREACHABLE");
	}

	@Test
	void conflictStatusYieldsUnknownOutcome() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/void"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(org.springframework.test.web.client.response.MockRestResponseCreators.withStatus(
						org.springframework.http.HttpStatus.CONFLICT));

		PspAuthorizationOutcome outcome = new PspVoidClient(builder.build()).voidOperation(CONTEXT, AUTHORIZATION_PSP_OPERATION_ID);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		assertThat(outcome.reasonCode()).isEqualTo("PSP_OPERATION_CONFLICT");
		server.verify();
	}
}
