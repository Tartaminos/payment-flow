package jhsoftware.paymentsflow.processor;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

@Component
@ConditionalOnProperty(name = "paymentflow.messaging.enabled", havingValue = "true")
class PspLookupClient {

	private static final Logger logger = LoggerFactory.getLogger(PspLookupClient.class);

	private final RestClient restClient;

	PspLookupClient(RestClient pspLookupRestClient) {
		this.restClient = pspLookupRestClient;
	}

	PspAuthorizationOutcome lookup(UUID pspOperationId) {
		try {
			return restClient.get()
					.uri("/simulator/operations/{id}", pspOperationId)
					.exchange((clientRequest, response) -> {
						HttpStatusCode status = response.getStatusCode();
						if (status.is2xxSuccessful()) {
							return toOutcome(response.bodyTo(PspAuthorizeResponseBody.class));
						}
						if (status.value() == 404) {
							return PspAuthorizationOutcome.unknown("PSP_LOOKUP_NOT_FOUND");
						}
						return PspAuthorizationOutcome.unknown("PSP_LOOKUP_UNAVAILABLE");
					});
		}
		catch (RestClientResponseException exception) {
			logger.warn(
					"PSP lookup for operation {} returned an unexpected status {}",
					pspOperationId,
					exception.getStatusCode());
			return PspAuthorizationOutcome.unknown("PSP_LOOKUP_UNEXPECTED_STATUS");
		}
		catch (ResourceAccessException exception) {
			logger.warn(
					"PSP lookup for operation {} could not be confirmed ({})",
					pspOperationId,
					exception.getMessage());
			return PspAuthorizationOutcome.unknown("PSP_LOOKUP_UNREACHABLE");
		}
	}

	private PspAuthorizationOutcome toOutcome(PspAuthorizeResponseBody body) {
		if (body == null || body.result() == null) {
			return PspAuthorizationOutcome.unknown("PSP_LOOKUP_UNRECOGNIZED_RESULT");
		}
		return switch (body.result()) {
			case "SUCCEEDED" -> PspAuthorizationOutcome.succeeded(body.pspOperationId().toString());
			case "REJECTED" -> PspAuthorizationOutcome.rejected(body.pspOperationId().toString(), body.reasonCode());
			case "FAILED" -> PspAuthorizationOutcome.failed(body.pspOperationId().toString(), body.reasonCode());
			case "PROCESSING" -> PspAuthorizationOutcome.unknown("PSP_LOOKUP_PROCESSING");
			default -> PspAuthorizationOutcome.unknown("PSP_LOOKUP_UNRECOGNIZED_RESULT");
		};
	}
}
