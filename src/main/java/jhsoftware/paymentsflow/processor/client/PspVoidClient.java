package jhsoftware.paymentsflow.processor.client;

import jhsoftware.paymentsflow.processor.dto.PspAuthorizeResponseDto;
import jhsoftware.paymentsflow.processor.dto.PspVoidRequestDto;
import jhsoftware.paymentsflow.processor.model.PaymentOperationContext;
import jhsoftware.paymentsflow.processor.model.PspAuthorizationOutcome;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

public class PspVoidClient {

	private static final Logger logger = LoggerFactory.getLogger(PspVoidClient.class);

	private final RestClient restClient;

	public PspVoidClient(RestClient restClient) {
		this.restClient = restClient;
	}

	public PspAuthorizationOutcome voidOperation(PaymentOperationContext context, UUID authorizationPspOperationId) {
		PspVoidRequestDto request = new PspVoidRequestDto(
				context.operationId(),
				context.merchantId(),
				context.paymentId(),
				context.amountMinor(),
				context.currency(),
				authorizationPspOperationId);

		try {
			return restClient.post()
					.uri("/simulator/operations/void")
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
					"PSP void for operation {} returned an unexpected status {}",
					context.operationId(),
					exception.getStatusCode());
			return PspAuthorizationOutcome.unknown("PSP_UNEXPECTED_STATUS");
		}
		catch (ResourceAccessException exception) {
			logger.warn(
					"PSP void for operation {} could not be confirmed ({})",
					context.operationId(),
					exception.getMessage());
			return PspAuthorizationOutcome.unknown("PSP_UNREACHABLE");
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
			logger.error("PSP rejected void request as PSP_OPERATION_CONFLICT");
			return PspAuthorizationOutcome.unknown("PSP_OPERATION_CONFLICT");
		}
		return PspAuthorizationOutcome.unknown("PSP_UNAVAILABLE");
	}
}
