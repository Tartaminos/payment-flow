package jhsoftware.paymentsflow.payment.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServiceUnavailable;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestClient;

import jhsoftware.paymentsflow.payment.model.AuthorizationValidityEvidence;
import jhsoftware.paymentsflow.payment.model.AuthorizationValidityStatus;

class CaptureAuthorizationValidityClientTests {

	@Test
	void activeEvidenceYieldsActiveStatusWithEvidenceTimestamp() {
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

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(builder.build())
				.checkValidity(authorizationPspOperationId);

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.ACTIVE);
		assertThat(evidence.validityEvidenceAt()).isEqualTo(Instant.parse("2026-10-27T10:00:00Z"));
		server.verify();
	}

	@Test
	void expiredEvidenceYieldsExpiredStatusWithEvidenceTimestamp() {
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

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(builder.build())
				.checkValidity(authorizationPspOperationId);

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.EXPIRED);
		assertThat(evidence.validityEvidenceAt()).isEqualTo(Instant.parse("2026-10-27T10:00:00Z"));
	}

	@Test
	void missingValidityFieldYieldsUnknownStatus() {
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

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(builder.build())
				.checkValidity(authorizationPspOperationId);

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.UNKNOWN);
	}

	@Test
	void unrecognizedValidityValueYieldsUnknownStatus() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(withSuccess(
						"""
						{"pspOperationId":"%s","result":"SUCCEEDED","reasonCode":null,"validity":"SOMETHING_ELSE","validityEvidenceAt":null}
						"""
								.formatted(authorizationPspOperationId),
						MediaType.APPLICATION_JSON));

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(builder.build())
				.checkValidity(authorizationPspOperationId);

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.UNKNOWN);
	}

	@Test
	void notFoundResponseYieldsUnknownStatus() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(MockRestResponseCreators.withStatus(org.springframework.http.HttpStatus.NOT_FOUND)
						.contentType(MediaType.APPLICATION_JSON)
						.body("{\"code\":\"PSP_OPERATION_NOT_FOUND\"}"));

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(builder.build())
				.checkValidity(authorizationPspOperationId);

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.UNKNOWN);
	}

	@Test
	void serviceUnavailableResponseYieldsUnknownStatus() {
		UUID authorizationPspOperationId = UUID.randomUUID();
		RestClient.Builder builder = RestClient.builder();
		MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
		server.expect(requestTo("/simulator/operations/" + authorizationPspOperationId))
				.andRespond(withServiceUnavailable());

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(builder.build())
				.checkValidity(authorizationPspOperationId);

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.UNKNOWN);
	}

	@Test
	void unreachableServerYieldsUnknownStatus() {
		RestClient restClient = RestClient.builder().baseUrl("http://127.0.0.1:1").build();

		AuthorizationValidityEvidence evidence = new CaptureAuthorizationValidityClient(restClient)
				.checkValidity(UUID.randomUUID());

		assertThat(evidence.status()).isEqualTo(AuthorizationValidityStatus.UNKNOWN);
	}
}
