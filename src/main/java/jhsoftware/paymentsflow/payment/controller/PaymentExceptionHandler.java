package jhsoftware.paymentsflow.payment.controller;

import jakarta.validation.ConstraintViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import jhsoftware.paymentsflow.payment.dto.ApiErrorDto;
import jhsoftware.paymentsflow.payment.exception.AuthorizationExpiredException;
import jhsoftware.paymentsflow.payment.exception.IdempotencyKeyConflictException;
import jhsoftware.paymentsflow.payment.exception.InvalidPaymentIdException;
import jhsoftware.paymentsflow.payment.exception.InvalidPaymentRequestException;
import jhsoftware.paymentsflow.payment.exception.PaymentAlreadyCapturedException;
import jhsoftware.paymentsflow.payment.exception.PaymentNotEligibleException;
import jhsoftware.paymentsflow.payment.exception.PaymentNotFoundException;
import jhsoftware.paymentsflow.payment.exception.PaymentOperationInProgressException;
import jhsoftware.paymentsflow.payment.exception.PspStatusUnavailableException;

@RestControllerAdvice(assignableTypes = PaymentController.class)
class PaymentExceptionHandler {

	private static final Logger logger = LoggerFactory.getLogger(PaymentExceptionHandler.class);

	private static final ApiErrorDto INVALID_REQUEST = new ApiErrorDto(
			"INVALID_PAYMENT_REQUEST",
			"The payment request is invalid.");
	private static final ApiErrorDto IDEMPOTENCY_KEY_CONFLICT = new ApiErrorDto(
			"IDEMPOTENCY_KEY_CONFLICT",
			"The idempotency key is already used for different payment details.");
	private static final ApiErrorDto INVALID_PAYMENT_ID = new ApiErrorDto(
			"INVALID_PAYMENT_ID",
			"The payment ID is not a valid identifier.");
	private static final ApiErrorDto PAYMENT_NOT_FOUND = new ApiErrorDto(
			"PAYMENT_NOT_FOUND",
			"The payment could not be found.");
	private static final ApiErrorDto PAYMENT_ALREADY_CAPTURED = new ApiErrorDto(
			"PAYMENT_ALREADY_CAPTURED",
			"The payment has already been captured.");
	private static final ApiErrorDto PAYMENT_NOT_ELIGIBLE = new ApiErrorDto(
			"PAYMENT_NOT_ELIGIBLE",
			"The payment is not eligible for this operation.");
	private static final ApiErrorDto PAYMENT_OPERATION_IN_PROGRESS = new ApiErrorDto(
			"PAYMENT_OPERATION_IN_PROGRESS",
			"A conflicting payment operation is already in progress.");
	private static final ApiErrorDto PSP_STATUS_UNAVAILABLE = new ApiErrorDto(
			"PSP_STATUS_UNAVAILABLE",
			"The PSP could not confirm the authorization is still active.");
	private static final ApiErrorDto AUTHORIZATION_EXPIRED = new ApiErrorDto(
			"AUTHORIZATION_EXPIRED",
			"The authorization has expired and can no longer be captured.");

	@ExceptionHandler({
			InvalidPaymentRequestException.class,
			MethodArgumentNotValidException.class,
			HttpMessageNotReadableException.class,
			MissingRequestHeaderException.class,
			ConstraintViolationException.class,
			HandlerMethodValidationException.class
	})
	ResponseEntity<ApiErrorDto> invalidRequest(Exception exception) {
		return ResponseEntity.badRequest().body(INVALID_REQUEST);
	}

	@ExceptionHandler(IdempotencyKeyConflictException.class)
	ResponseEntity<ApiErrorDto> idempotencyConflict() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(IDEMPOTENCY_KEY_CONFLICT);
	}

	@ExceptionHandler(InvalidPaymentIdException.class)
	ResponseEntity<ApiErrorDto> invalidPaymentId() {
		return ResponseEntity.badRequest().body(INVALID_PAYMENT_ID);
	}

	@ExceptionHandler(PaymentNotFoundException.class)
	ResponseEntity<ApiErrorDto> paymentNotFound() {
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(PAYMENT_NOT_FOUND);
	}

	@ExceptionHandler(PaymentAlreadyCapturedException.class)
	ResponseEntity<ApiErrorDto> paymentAlreadyCaptured() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(PAYMENT_ALREADY_CAPTURED);
	}

	@ExceptionHandler(PaymentNotEligibleException.class)
	ResponseEntity<ApiErrorDto> paymentNotEligible() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(PAYMENT_NOT_ELIGIBLE);
	}

	@ExceptionHandler(PaymentOperationInProgressException.class)
	ResponseEntity<ApiErrorDto> paymentOperationInProgress() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(PAYMENT_OPERATION_IN_PROGRESS);
	}

	@ExceptionHandler(AuthorizationExpiredException.class)
	ResponseEntity<ApiErrorDto> authorizationExpired() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(AUTHORIZATION_EXPIRED);
	}

	@ExceptionHandler(PspStatusUnavailableException.class)
	ResponseEntity<ApiErrorDto> pspStatusUnavailable() {
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(PSP_STATUS_UNAVAILABLE);
	}

	@ExceptionHandler(DataIntegrityViolationException.class)
	ResponseEntity<ApiErrorDto> dataIntegrity(DataIntegrityViolationException exception) {
		if (causedByConstraint(exception, "uk_idempotency_keys_scope")) {
			logger.error("Payment admission failed because the idempotency uniqueness constraint was violated");
			return internalServerError();
		}
		logger.error("Payment admission failed because a database constraint was violated", exception);
		return internalServerError();
	}

	@ExceptionHandler(Exception.class)
	ResponseEntity<ApiErrorDto> unexpected(Exception exception) {
		logger.error("Payment admission failed unexpectedly", exception);
		return internalServerError();
	}

	private boolean causedByConstraint(Throwable throwable, String constraintName) {
		for (Throwable current = throwable; current != null; current = current.getCause()) {
			if (current.getMessage() != null && current.getMessage().contains(constraintName)) {
				return true;
			}
		}
		return false;
	}

	private ResponseEntity<ApiErrorDto> internalServerError() {
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ApiErrorDto(
				"INTERNAL_SERVER_ERROR",
				"The payment request could not be processed."));
	}
}
