package jhsoftware.pspsimulator.controller;

import jhsoftware.pspsimulator.dto.AuthorizeOperationRequestDto;
import jhsoftware.pspsimulator.dto.CaptureOperationRequestDto;
import jhsoftware.pspsimulator.dto.OperationDecisionResponseDto;
import jhsoftware.pspsimulator.dto.VoidOperationRequestDto;
import jhsoftware.pspsimulator.exception.PspOperationNotFoundException;
import jhsoftware.pspsimulator.model.AuthorizeOperationResult;
import jhsoftware.pspsimulator.model.CaptureOperationResult;
import jhsoftware.pspsimulator.model.VoidOperationResult;
import jhsoftware.pspsimulator.service.PspOperationService;

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
	ResponseEntity<OperationDecisionResponseDto> authorize(@Valid @RequestBody AuthorizeOperationRequestDto request) {
		AuthorizeOperationResult result = service.authorize(request);
		if (result.stillProcessing()) {
			return ResponseEntity.status(HttpStatus.ACCEPTED).body(result.decision());
		}
		return ResponseEntity.ok(result.decision());
	}

	@PostMapping("/simulator/operations/capture")
	ResponseEntity<OperationDecisionResponseDto> capture(@Valid @RequestBody CaptureOperationRequestDto request) {
		CaptureOperationResult result = service.capture(request);
		if (result.stillProcessing()) {
			return ResponseEntity.status(HttpStatus.ACCEPTED).body(result.decision());
		}
		return ResponseEntity.ok(result.decision());
	}

	@PostMapping("/simulator/operations/void")
	ResponseEntity<OperationDecisionResponseDto> voidOperation(@Valid @RequestBody VoidOperationRequestDto request) {
		VoidOperationResult result = service.voidOperation(request);
		if (result.stillProcessing()) {
			return ResponseEntity.status(HttpStatus.ACCEPTED).body(result.decision());
		}
		return ResponseEntity.ok(result.decision());
	}

	@GetMapping("/simulator/operations/{pspOperationId}")
	ResponseEntity<OperationDecisionResponseDto> findByPspOperationId(@PathVariable UUID pspOperationId) {
		return service.findDecision(pspOperationId)
				.map(ResponseEntity::ok)
				.orElseThrow(PspOperationNotFoundException::new);
	}
}
