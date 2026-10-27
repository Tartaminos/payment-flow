package jhsoftware.paymentsflow.payment;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

@Component
class CaptureAuthorizationValidityClient {

	private static final Logger logger = LoggerFactory.getLogger(CaptureAuthorizationValidityClient.class);

	private final RestClient restClient;

	CaptureAuthorizationValidityClient(RestClient captureAuthorizationValidityRestClient) {
		this.restClient = captureAuthorizationValidityRestClient;
	}

	AuthorizationValidityEvidence checkValidity(UUID authorizationPspOperationId) {
		try {
			return restClient.get()
					.uri("/simulator/operations/{id}", authorizationPspOperationId)
					.exchange((clientRequest, response) -> {
						if (!response.getStatusCode().is2xxSuccessful()) {
							return AuthorizationValidityEvidence.unknown();
						}
						CaptureValidityResponseBody body = response.bodyTo(CaptureValidityResponseBody.class);
						return toEvidence(body);
					});
		}
		catch (RestClientResponseException exception) {
			logger.warn(
					"PSP authorization validity lookup for operation {} returned an unexpected status {}",
					authorizationPspOperationId,
					exception.getStatusCode());
			return AuthorizationValidityEvidence.unknown();
		}
		catch (ResourceAccessException exception) {
			logger.warn(
					"PSP authorization validity lookup for operation {} could not be confirmed ({})",
					authorizationPspOperationId,
					exception.getMessage());
			return AuthorizationValidityEvidence.unknown();
		}
	}

	private AuthorizationValidityEvidence toEvidence(CaptureValidityResponseBody body) {
		if (body == null || body.validity() == null) {
			return AuthorizationValidityEvidence.unknown();
		}
		return switch (body.validity()) {
			case "ACTIVE" -> new AuthorizationValidityEvidence(AuthorizationValidityStatus.ACTIVE, body.validityEvidenceAt());
			case "EXPIRED" -> new AuthorizationValidityEvidence(AuthorizationValidityStatus.EXPIRED, body.validityEvidenceAt());
			default -> AuthorizationValidityEvidence.unknown();
		};
	}
}
