package jhsoftware.pspsimulator;

import java.util.UUID;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
class PspOperationController {

	private final PspOperationService service;

	PspOperationController(PspOperationService service) {
		this.service = service;
	}

	@PostMapping("/simulator/operations/authorize")
	ResponseEntity<OperationDecisionResponse> authorize(@Valid @RequestBody AuthorizeOperationRequest request) {
		AuthorizeOperationResult result = service.authorize(request);
		if (result.stillProcessing()) {
			return ResponseEntity.status(HttpStatus.ACCEPTED).body(result.decision());
		}
		return ResponseEntity.ok(result.decision());
	}

	@PostMapping("/simulator/operations/capture")
	ResponseEntity<OperationDecisionResponse> capture(@Valid @RequestBody CaptureOperationRequest request) {
		CaptureOperationResult result = service.capture(request);
		if (result.stillProcessing()) {
			return ResponseEntity.status(HttpStatus.ACCEPTED).body(result.decision());
		}
		return ResponseEntity.ok(result.decision());
	}

	@GetMapping("/simulator/operations/{pspOperationId}")
	ResponseEntity<OperationDecisionResponse> findByPspOperationId(@PathVariable UUID pspOperationId) {
		return service.findDecision(pspOperationId)
				.map(ResponseEntity::ok)
				.orElseThrow(PspOperationNotFoundException::new);
	}
}
