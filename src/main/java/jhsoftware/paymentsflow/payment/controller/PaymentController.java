package jhsoftware.paymentsflow.payment.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jhsoftware.paymentsflow.payment.dto.CreatePaymentRequestDto;
import jhsoftware.paymentsflow.payment.dto.CreatePaymentResponseDto;
import jhsoftware.paymentsflow.payment.dto.PaymentStatusResponseDto;
import jhsoftware.paymentsflow.payment.exception.InvalidPaymentRequestException;
import jhsoftware.paymentsflow.payment.service.CaptureAdmissionService;
import jhsoftware.paymentsflow.payment.service.PaymentAdmissionService;
import jhsoftware.paymentsflow.payment.service.PaymentStatusService;
import jhsoftware.paymentsflow.payment.service.VoidAdmissionService;

@Validated
@RestController
@RequestMapping("/payments")
class PaymentController {

	private final PaymentAdmissionService paymentAdmissionService;
	private final PaymentStatusService paymentStatusService;
	private final CaptureAdmissionService captureAdmissionService;
	private final VoidAdmissionService voidAdmissionService;

	PaymentController(
			PaymentAdmissionService paymentAdmissionService,
			PaymentStatusService paymentStatusService,
			CaptureAdmissionService captureAdmissionService,
			VoidAdmissionService voidAdmissionService) {
		this.paymentAdmissionService = paymentAdmissionService;
		this.paymentStatusService = paymentStatusService;
		this.captureAdmissionService = captureAdmissionService;
		this.voidAdmissionService = voidAdmissionService;
	}

	@PostMapping
	ResponseEntity<CreatePaymentResponseDto> create(
			@RequestHeader("Idempotency-Key")
			@NotBlank @Pattern(regexp = "[\\x20-\\x7e]{1,150}") String idempotencyKey,
			@Valid @RequestBody CreatePaymentRequestDto request) {
		CreatePaymentResponseDto response = paymentAdmissionService.admit(request.toCommand(idempotencyKey));
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}

	@GetMapping("/{paymentId}")
	ResponseEntity<PaymentStatusResponseDto> read(@PathVariable String paymentId) {
		return ResponseEntity.ok(paymentStatusService.getStatus(paymentId));
	}

	@PostMapping("/{paymentId}/capture")
	ResponseEntity<CreatePaymentResponseDto> capture(
			@PathVariable String paymentId,
			@RequestHeader("Idempotency-Key")
			@NotBlank @Pattern(regexp = "[\\x20-\\x7e]{1,150}") String idempotencyKey,
			@RequestBody(required = false) byte[] body) {
		if (body != null && body.length > 0) {
			throw new InvalidPaymentRequestException();
		}
		CreatePaymentResponseDto response = captureAdmissionService.admit(paymentId, idempotencyKey);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}

	@PostMapping("/{paymentId}/void")
	ResponseEntity<CreatePaymentResponseDto> voidPayment(
			@PathVariable String paymentId,
			@RequestHeader("Idempotency-Key")
			@NotBlank @Pattern(regexp = "[\\x20-\\x7e]{1,150}") String idempotencyKey,
			@RequestBody(required = false) byte[] body) {
		if (body != null && body.length > 0) {
			throw new InvalidPaymentRequestException();
		}
		CreatePaymentResponseDto response = voidAdmissionService.admit(paymentId, idempotencyKey);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}
}
