package jhsoftware.pspsimulator.controller;

import jhsoftware.pspsimulator.dto.SimulatorApiErrorDto;
import jhsoftware.pspsimulator.exception.PspOperationConflictException;
import jhsoftware.pspsimulator.exception.PspOperationNotFoundException;
import jhsoftware.pspsimulator.exception.PspReplyDroppedFaultException;
import jhsoftware.pspsimulator.exception.PspUnprocessedFaultException;

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
	ResponseEntity<SimulatorApiErrorDto> invalidRequest() {
		return ResponseEntity.badRequest().body(new SimulatorApiErrorDto(
				"INVALID_SIMULATOR_REQUEST",
				"The simulator request is invalid."));
	}

	@ExceptionHandler(PspOperationConflictException.class)
	ResponseEntity<SimulatorApiErrorDto> conflict() {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(new SimulatorApiErrorDto(
				"PSP_OPERATION_CONFLICT",
				"The PSP operation identity is already used with different parameters."));
	}

	@ExceptionHandler(PspOperationNotFoundException.class)
	ResponseEntity<SimulatorApiErrorDto> notFound() {
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new SimulatorApiErrorDto(
				"PSP_OPERATION_NOT_FOUND",
				"No PSP operation decision was found for that identity."));
	}

	@ExceptionHandler(PspReplyDroppedFaultException.class)
	ResponseEntity<SimulatorApiErrorDto> replyDropped() {
		logger.info("Simulated PSP fault: decision committed but reply dropped");
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new SimulatorApiErrorDto(
				"PSP_UNAVAILABLE",
				"The PSP reply could not be delivered."));
	}

	@ExceptionHandler(PspUnprocessedFaultException.class)
	ResponseEntity<SimulatorApiErrorDto> unprocessed() {
		logger.info("Simulated PSP fault: request was not processed");
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new SimulatorApiErrorDto(
				"PSP_UNAVAILABLE",
				"The PSP could not process the request."));
	}

	@ExceptionHandler(Exception.class)
	ResponseEntity<SimulatorApiErrorDto> unexpected(Exception exception) {
		logger.error("Simulator request failed unexpectedly", exception);
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new SimulatorApiErrorDto(
				"INTERNAL_SERVER_ERROR",
				"The simulator request could not be processed."));
	}
}
