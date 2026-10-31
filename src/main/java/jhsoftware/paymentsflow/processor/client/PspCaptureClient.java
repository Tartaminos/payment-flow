package jhsoftware.paymentsflow.processor.client;

import jhsoftware.paymentsflow.processor.dto.PspAuthorizeResponseDto;
import jhsoftware.paymentsflow.processor.dto.PspCaptureRequestDto;
import jhsoftware.paymentsflow.processor.model.PaymentOperationContext;
import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcome;

import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

public class PspCaptureClient {

	private static final Logger logger = LoggerFactory.getLogger(PspCaptureClient.class);
	private static final String AUTHORIZATION_EXPIRED_REASON = "AUTHORIZATION_EXPIRED";

	private final RestClient restClient;

	public PspCaptureClient(RestClient restClient) {
		this.restClient = restClient;
	}

	public PspAuthorizationOutcome capture(PaymentOperationContext context, UUID authorizationPspOperationId) {
		Optional<PspAuthorizationOutcome> validityShortCircuit = checkValidity(context, authorizationPspOperationId);
		if (validityShortCircuit.isPresent()) {
			return validityShortCircuit.get();
		}

		PspCaptureRequestDto request = new PspCaptureRequestDto(
				context.operationId(),
				context.merchantId(),
				context.paymentId(),
				context.amountMinor(),
				context.currency(),
				authorizationPspOperationId);

		try {
			return restClient.post()
					.uri("/simulator/operations/capture")
					.body(request)
					.exchange((clientRequest, response) -> {
						HttpStatusCode status = response.getStatusCode();
						if (status.is2xxSuccessful()) {
							return toOutcome(status, response.bodyTo(PspAuthorizeResponseDto.class));
						}
						return toFailureOutcome(status);
					});
		}
		catch (RestClientResponseException exception) {
			logger.warn(
					"PSP capture for operation {} returned an unexpected status {}",
					context.operationId(),
					exception.getStatusCode());
			return PspAuthorizationOutcome.unknown("PSP_UNEXPECTED_STATUS");
		}
		catch (ResourceAccessException exception) {
			logger.warn(
					"PSP capture for operation {} could not be confirmed ({})",
					context.operationId(),
					exception.getMessage());
			return PspAuthorizationOutcome.unknown("PSP_UNREACHABLE");
		}
	}

	private Optional<PspAuthorizationOutcome> checkValidity(PaymentOperationContext context, UUID authorizationPspOperationId) {
		try {
			return restClient.get()
					.uri("/simulator/operations/{id}", authorizationPspOperationId)
					.exchange((clientRequest, response) -> {
						HttpStatusCode status = response.getStatusCode();
						if (!status.is2xxSuccessful()) {
							return Optional.of(PspAuthorizationOutcome.unknown("PSP_VALIDITY_LOOKUP_UNAVAILABLE"));
						}
						PspAuthorizeResponseDto body = response.bodyTo(PspAuthorizeResponseDto.class);
						if (body == null || body.validity() == null) {
							return Optional.of(PspAuthorizationOutcome.unknown("PSP_VALIDITY_LOOKUP_UNRECOGNIZED_RESULT"));
						}
						if ("EXPIRED".equals(body.validity())) {
							return Optional.of(PspAuthorizationOutcome.rejected(
									context.operationId().toString(), AUTHORIZATION_EXPIRED_REASON));
						}
						return Optional.empty();
					});
		}
		catch (RestClientResponseException exception) {
			logger.warn(
					"PSP authorization validity recheck for operation {} returned an unexpected status {}",
					authorizationPspOperationId,
					exception.getStatusCode());
			return Optional.of(PspAuthorizationOutcome.unknown("PSP_VALIDITY_LOOKUP_UNEXPECTED_STATUS"));
		}
		catch (ResourceAccessException exception) {
			logger.warn(
					"PSP authorization validity recheck for operation {} could not be confirmed ({})",
					authorizationPspOperationId,
					exception.getMessage());
			return Optional.of(PspAuthorizationOutcome.unknown("PSP_VALIDITY_LOOKUP_UNREACHABLE"));
		}
	}

	private PspAuthorizationOutcome toOutcome(HttpStatusCode status, PspAuthorizeResponseDto body) {
		if (status.value() == 202 || body == null || body.result() == null) {
			return PspAuthorizationOutcome.unknown("PSP_PROCESSING");
		}
		return switch (body.result()) {
			case "SUCCEEDED" -> PspAuthorizationOutcome.succeeded(body.pspOperationId().toString());
			case "REJECTED" -> PspAuthorizationOutcome.rejected(body.pspOperationId().toString(), body.reasonCode());
			case "FAILED" -> PspAuthorizationOutcome.failed(body.pspOperationId().toString(), body.reasonCode());
			case "PROCESSING" -> PspAuthorizationOutcome.unknown("PSP_PROCESSING");
			default -> PspAuthorizationOutcome.unknown("PSP_UNRECOGNIZED_RESULT");
		};
	}

	private PspAuthorizationOutcome toFailureOutcome(HttpStatusCode status) {
		if (status.value() == 409) {
			logger.error("PSP rejected capture request as PSP_OPERATION_CONFLICT");
			return PspAuthorizationOutcome.unknown("PSP_OPERATION_CONFLICT");
		}
		return PspAuthorizationOutcome.unknown("PSP_UNAVAILABLE");
	}
}
