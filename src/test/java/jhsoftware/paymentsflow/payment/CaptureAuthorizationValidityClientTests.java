package jhsoftware.paymentsflow.payment;

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
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestClient;

class CaptureAuthorizationValidityClientTests {

	@Test
	void activeEvidenceYieldsAuthorizationActive() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andExpect(method(HttpMethod.GET))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":"ACTIVE","validityEvidenceAt":"2026-10-27T10:00:00Z"}
						"""
								.formatted(authorizationPspOperationId),
						MediaType.APPLICATION_JSON));

		boolean active = new CaptureAuthorizationValidityClient(builder.build())
				.isAuthorizationActive(authorizationPspOperationId);

		assertThat(active).isTrue();
		server.verify();
	}

	@Test
	void expiredEvidenceYieldsAuthorizationNotActive() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":"EXPIRED","validityEvidenceAt":"2026-10-27T10:00:00Z"}
						"""
								.formatted(authorizationPspOperationId),
						MediaType.APPLICATION_JSON));

		boolean active = new CaptureAuthorizationValidityClient(builder.build())
				.isAuthorizationActive(authorizationPspOperationId);

		assertThat(active).isFalse();
	}

	@Test
	void missingValidityFieldYieldsAuthorizationNotActive() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":null,"validityEvidenceAt":null}
						"""
								.formatted(authorizationPspOperationId),
						MediaType.APPLICATION_JSON));

		boolean active = new CaptureAuthorizationValidityClient(builder.build())
				.isAuthorizationActive(authorizationPspOperationId);

		assertThat(active).isFalse();
	}

	@Test
	void notFoundResponseYieldsAuthorizationNotActive() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(MockRestResponseCreators.withStatus(org.springframework.http.HttpStatus.NOT_FOUND)
						.contentType(MediaType.APPLICATION_JSON)
						.body("{\"code\":\"PSP_OPERATION_NOT_FOUND\"}"));

		boolean active = new CaptureAuthorizationValidityClient(builder.build())
				.isAuthorizationActive(authorizationPspOperationId);

		assertThat(active).isFalse();
	}

	@Test
	void serviceUnavailableResponseYieldsAuthorizationNotActive() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(withServiceUnavailable());

		boolean active = new CaptureAuthorizationValidityClient(builder.build())
				.isAuthorizationActive(authorizationPspOperationId);

		assertThat(active).isFalse();
	}

	@Test
	void unreachableServerYieldsAuthorizationNotActive() {
		RestClient restClient = RestClient.builder().baseUrl("http://127.0.0.1:1").build();

		boolean active = new CaptureAuthorizationValidityClient(restClient)
				.isAuthorizationActive(UUID.randomUUID());

		assertThat(active).isFalse();
	}
}
