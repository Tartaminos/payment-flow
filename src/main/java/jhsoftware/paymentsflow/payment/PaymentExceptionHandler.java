package jhsoftware.paymentsflow.payment;

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

@RestControllerAdvice(assignableTypes = PaymentController.class)
class PaymentExceptionHandler {

	private static final Logger logger = LoggerFactory.getLogger(PaymentExceptionHandler.class);

	private static final ApiError INVALID_REQUEST = new ApiError(
			"INVALID_PAYMENT_REQUEST",
			"The payment request is invalid.");

	@ExceptionHandler({
			InvalidPaymentRequestException.class,
			MethodArgumentNotValidException.class,
			HttpMessageNotReadableException.class,
			MissingRequestHeaderException.class,
			ConstraintViolationException.class,
			HandlerMethodValidationException.class
	})
	ResponseEntity<ApiError> invalidRequest(Exception exception) {
		return ResponseEntity.badRequest().body(INVALID_REQUEST);
	}

	@ExceptionHandler(DataIntegrityViolationException.class)
	ResponseEntity<ApiError> dataIntegrity(DataIntegrityViolationException exception) {
		if (causedByConstraint(exception, "uk_idempotency_keys_scope")) {
			return ResponseEntity.status(HttpStatus.CONFLICT).body(new ApiError(
					"IDEMPOTENCY_KEY_CONFLICT",
					"The idempotency key has already been used."));
		}
		logger.error("Payment admission failed because a database constraint was violated", exception);
		return internalServerError();
	}

	@ExceptionHandler(Exception.class)
	ResponseEntity<ApiError> unexpected(Exception exception) {
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

	private ResponseEntity<ApiError> internalServerError() {
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ApiError(
				"INTERNAL_SERVER_ERROR",
				"The payment request could not be processed."));
	}
}
