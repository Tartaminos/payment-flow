package jhsoftware.pspsimulator;

import jakarta.validation.ConstraintViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

@RestControllerAdvice(assignableTypes = PspOperationController.class)
class SimulatorExceptionHandler {

	private static final Logger logger = LoggerFactory.getLogger(SimulatorExceptionHandler.class);

	@ExceptionHandler({
			MethodArgumentNotValidException.class,
			HttpMessageNotReadableException.class,
			ConstraintViolationException.class,
			HandlerMethodValidationException.class
	})
	ResponseEntity<SimulatorApiError> invalidRequest() {
		return ResponseEntity.badRequest().body(new SimulatorApiError(
				"INVALID_SIMULATOR_REQUEST",
				"The simulator request is invalid."));
	}

	@ExceptionHandler(PspOperationConflictException.class)
	ResponseEntity<SimulatorApiError> conflict() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(new SimulatorApiError(
				"PSP_OPERATION_CONFLICT",
				"The PSP operation identity is already used with different parameters."));
	}

	@ExceptionHandler(PspOperationNotFoundException.class)
	ResponseEntity<SimulatorApiError> notFound() {
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new SimulatorApiError(
				"PSP_OPERATION_NOT_FOUND",
				"No PSP operation decision was found for that identity."));
	}

	@ExceptionHandler(PspReplyDroppedFaultException.class)
	ResponseEntity<SimulatorApiError> replyDropped() {
		logger.info("Simulated PSP fault: decision committed but reply dropped");
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new SimulatorApiError(
				"PSP_UNAVAILABLE",
				"The PSP reply could not be delivered."));
	}

	@ExceptionHandler(PspUnprocessedFaultException.class)
	ResponseEntity<SimulatorApiError> unprocessed() {
		logger.info("Simulated PSP fault: request was not processed");
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new SimulatorApiError(
				"PSP_UNAVAILABLE",
				"The PSP could not process the request."));
	}

	@ExceptionHandler(Exception.class)
	ResponseEntity<SimulatorApiError> unexpected(Exception exception) {
		logger.error("Simulator request failed unexpectedly", exception);
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new SimulatorApiError(
				"INTERNAL_SERVER_ERROR",
				"The simulator request could not be processed."));
	}
}
