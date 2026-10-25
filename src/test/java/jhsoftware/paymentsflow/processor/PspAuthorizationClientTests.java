package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServiceUnavailable;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class PspAuthorizationClientTests {

	private static final PaymentOperationContext CONTEXT = new PaymentOperationContext(
			UUID.randomUUID(), "AUTHORIZE", "PENDING", 50000L, "BRL", UUID.randomUUID(), UUID.randomUUID(), "CREATED");

	@Test
	void succeededResponseYieldsSucceededOutcomeWithPspReference() {
		UUID pspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/authorize"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"SUCCEEDED\",\"reasonCode\":null}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(builder.build()).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.SUCCEEDED);
		assertThat(outcome.pspReference()).isEqualTo(pspOperationId.toString());
		server.verify();
	}

	@Test
	void rejectedResponseYieldsRejectedOutcomeWithReasonCode() {
		UUID pspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/authorize"))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"REJECTED\",\"reasonCode\":\"CARD_DECLINED\"}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(builder.build()).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.REJECTED);
		assertThat(outcome.reasonCode()).isEqualTo("CARD_DECLINED");
	}

	@Test
	void failedResponseYieldsFailedOutcomeWithReasonCode() {
		UUID pspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/authorize"))
				.andRespond(withSuccess(
						"{\"pspOperationId\":\"%s\",\"result\":\"FAILED\",\"reasonCode\":\"NON_EXECUTION_CONFIRMED\"}".formatted(pspOperationId),
						MediaType.APPLICATION_JSON));

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(builder.build()).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.FAILED);
		assertThat(outcome.reasonCode()).isEqualTo("NON_EXECUTION_CONFIRMED");
	}

	@Test
	void acceptedProcessingResponseYieldsUnknownOutcomeRatherThanFailure() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/authorize"))
				.andRespond(withStatus(HttpStatus.ACCEPTED)
						.contentType(MediaType.APPLICATION_JSON)
						.body("{\"pspOperationId\":\"%s\",\"result\":\"PROCESSING\",\"reasonCode\":null}".formatted(UUID.randomUUID())));

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(builder.build()).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
	}

	@Test
	void conflictResponseYieldsUnknownOutcomeRatherThanFailure() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/authorize"))
				.andRespond(withStatus(HttpStatus.CONFLICT)
						.contentType(MediaType.APPLICATION_JSON)
						.body("{\"code\":\"PSP_OPERATION_CONFLICT\"}"));

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(builder.build()).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		assertThat(outcome.reasonCode()).isEqualTo("PSP_OPERATION_CONFLICT");
	}

	@Test
	void serviceUnavailableResponseYieldsUnknownOutcomeRatherThanFailure() {
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/authorize"))
				.andRespond(withServiceUnavailable());

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(builder.build()).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		assertThat(outcome.reasonCode()).isEqualTo("PSP_UNAVAILABLE");
	}

	@Test
	void unreachableServerYieldsUnknownOutcomeRatherThanFailure() {
		RestClient restClient = RestClient.builder().baseUrl("http://127.0.0.1:1").build();

		PspAuthorizationOutcome outcome = new PspAuthorizationClient(restClient).authorize(CONTEXT);

		assertThat(outcome.type()).isEqualTo(PspAuthorizationOutcomeType.UNKNOWN);
		assertThat(outcome.reasonCode()).isEqualTo("PSP_UNREACHABLE");
	}
}
