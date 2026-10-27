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

	boolean isAuthorizationActive(UUID authorizationPspOperationId) {
		try {
			return restClient.get()
					.uri("/simulator/operations/{id}", authorizationPspOperationId)
					.exchange((clientRequest, response) -> {
						if (!response.getStatusCode().is2xxSuccessful()) {
							return false;
						}
						CaptureValidityResponseBody body = response.bodyTo(CaptureValidityResponseBody.class);
						return body != null && "ACTIVE".equals(body.validity());
					});
		}
		catch (RestClientResponseException exception) {
			logger.warn(
					"PSP authorization validity lookup for operation {} returned an unexpected status {}",
					authorizationPspOperationId,
					exception.getStatusCode());
			return false;
		}
		catch (ResourceAccessException exception) {
			logger.warn(
					"PSP authorization validity lookup for operation {} could not be confirmed ({})",
					authorizationPspOperationId,
					exception.getMessage());
			return false;
		}
	}
}
