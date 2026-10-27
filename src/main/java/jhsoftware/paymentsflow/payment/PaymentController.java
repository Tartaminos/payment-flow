package jhsoftware.paymentsflow.payment;

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

@Validated
@RestController
@RequestMapping("/payments")
class PaymentController {

	private final PaymentAdmissionService paymentAdmissionService;
	private final PaymentStatusService paymentStatusService;
	private final CaptureAdmissionService captureAdmissionService;

	PaymentController(
			PaymentAdmissionService paymentAdmissionService,
			PaymentStatusService paymentStatusService,
			CaptureAdmissionService captureAdmissionService) {
		this.paymentAdmissionService = paymentAdmissionService;
		this.paymentStatusService = paymentStatusService;
		this.captureAdmissionService = captureAdmissionService;
	}

	@PostMapping
	ResponseEntity<CreatePaymentResponse> create(
			@RequestHeader("Idempotency-Key")
			@NotBlank @Pattern(regexp = "[\\x20-\\x7e]{1,150}") String idempotencyKey,
			@Valid @RequestBody CreatePaymentRequest request) {
		CreatePaymentResponse response = paymentAdmissionService.admit(request.toCommand(idempotencyKey));
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}

	@GetMapping("/{paymentId}")
	ResponseEntity<PaymentStatusResponse> read(@PathVariable String paymentId) {
		return ResponseEntity.ok(paymentStatusService.getStatus(paymentId));
	}

	@PostMapping("/{paymentId}/capture")
	ResponseEntity<CreatePaymentResponse> capture(
			@PathVariable String paymentId,
			@RequestHeader("Idempotency-Key")
			@NotBlank @Pattern(regexp = "[\\x20-\\x7e]{1,150}") String idempotencyKey,
			@RequestBody(required = false) byte[] body) {
		if (body != null && body.length > 0) {
			throw new InvalidPaymentRequestException();
		}
		CreatePaymentResponse response = captureAdmissionService.admit(paymentId, idempotencyKey);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}
}
