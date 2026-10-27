package jhsoftware.pspsimulator;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
class PspOperationService {

	private static final String AUTHORIZATION_EXPIRED_REASON = "AUTHORIZATION_EXPIRED";

	private final PspOperationRepository repository;
	private final AmountMinorFixtureOutcomeSelector fixtureSelector;
	private final CaptureFixtureOutcomeSelector captureFixtureSelector;
	private final VoidFixtureOutcomeSelector voidFixtureSelector;
	private final Clock clock;

	PspOperationService(
			PspOperationRepository repository,
			AmountMinorFixtureOutcomeSelector fixtureSelector,
			CaptureFixtureOutcomeSelector captureFixtureSelector,
			VoidFixtureOutcomeSelector voidFixtureSelector,
			Clock clock) {
		this.repository = repository;
		this.fixtureSelector = fixtureSelector;
		this.captureFixtureSelector = captureFixtureSelector;
		this.voidFixtureSelector = voidFixtureSelector;
		this.clock = clock;
	}

	AuthorizeOperationResult authorize(AuthorizeOperationRequest request) {
		Optional<PspOperationRecord> existing = repository.findByMerchantAndPspOperationId(
				request.merchantId(), request.pspOperationId());
		if (existing.isPresent()) {
			return toResult(resolveOrConflict(existing.get(), request));
		}

		PspFixtureOutcome fixtureOutcome = fixtureSelector.outcomeFor(request.amountMinor());
		if (fixtureOutcome == PspFixtureOutcome.FAULT_UNPROCESSED) {
			throw new PspUnprocessedFaultException();
		}

		String decision = decisionFor(fixtureOutcome);
		String decisionReason = decisionReasonFor(fixtureOutcome);
		String validity = validityFor(fixtureOutcome);
		Instant now = clock.instant();

		try {
			repository.insert(
					request.merchantId(),
					request.pspOperationId(),
					request.paymentId(),
					request.amountMinor(),
					request.currency(),
					decision,
					decisionReason,
					"AUTHORIZE",
					null,
					validity,
					now,
					now);
		}
		catch (DataIntegrityViolationException exception) {
			PspOperationRecord raced = repository.findByMerchantAndPspOperationId(
							request.merchantId(), request.pspOperationId())
					.orElseThrow(() -> exception);
			return toResult(resolveOrConflict(raced, request));
		}

		if (fixtureOutcome == PspFixtureOutcome.FAULT_COMMITTED_REPLY_DROPPED) {
			throw new PspReplyDroppedFaultException();
		}

		return new AuthorizeOperationResult(
				new OperationDecisionResponse(request.pspOperationId(), decision, decisionReason, validity, now),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	CaptureOperationResult capture(CaptureOperationRequest request) {
		Optional<PspOperationRecord> existing = repository.findByMerchantAndPspOperationId(
				request.merchantId(), request.pspOperationId());
		if (existing.isPresent()) {
			return toCaptureResult(resolveOrConflict(existing.get(), request));
		}

		PspOperationRecord authorization = repository.findByPspOperationId(request.authorizationPspOperationId())
				.orElse(null);
		boolean authorizationExpired = authorization != null && "EXPIRED".equals(authorization.validity());

		String decision;
		String decisionReason;
		PspFixtureOutcome fixtureOutcome = null;
		if (authorizationExpired) {
			decision = "REJECTED";
			decisionReason = AUTHORIZATION_EXPIRED_REASON;
		}
		else {
			fixtureOutcome = captureFixtureSelector.outcomeFor(request.amountMinor());
			if (fixtureOutcome == PspFixtureOutcome.FAULT_UNPROCESSED) {
				throw new PspUnprocessedFaultException();
			}
			decision = decisionFor(fixtureOutcome);
			decisionReason = decisionReasonFor(fixtureOutcome);
		}

		Instant now = clock.instant();

		try {
			repository.insert(
					request.merchantId(),
					request.pspOperationId(),
					request.paymentId(),
					request.amountMinor(),
					request.currency(),
					decision,
					decisionReason,
					"CAPTURE",
					request.authorizationPspOperationId(),
					null,
					null,
					now);
		}
		catch (DataIntegrityViolationException exception) {
			PspOperationRecord raced = repository.findByMerchantAndPspOperationId(
							request.merchantId(), request.pspOperationId())
					.orElseThrow(() -> exception);
			return toCaptureResult(resolveOrConflict(raced, request));
		}

		if (fixtureOutcome == PspFixtureOutcome.FAULT_COMMITTED_REPLY_DROPPED) {
			throw new PspReplyDroppedFaultException();
		}

		return new CaptureOperationResult(
				new OperationDecisionResponse(request.pspOperationId(), decision, decisionReason, null, now),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	VoidOperationResult voidOperation(VoidOperationRequest request) {
		Optional<PspOperationRecord> existing = repository.findByMerchantAndPspOperationId(
				request.merchantId(), request.pspOperationId());
		if (existing.isPresent()) {
			return toVoidResult(resolveOrConflict(existing.get(), request));
		}

		PspOperationRecord authorization = repository.findByPspOperationId(request.authorizationPspOperationId())
				.orElse(null);
		boolean authorizationExpired = authorization != null && "EXPIRED".equals(authorization.validity());

		String decision;
		String decisionReason;
		PspFixtureOutcome fixtureOutcome = null;
		if (authorizationExpired) {
			decision = "REJECTED";
			decisionReason = AUTHORIZATION_EXPIRED_REASON;
		}
		else {
			fixtureOutcome = voidFixtureSelector.outcomeFor(request.amountMinor());
			if (fixtureOutcome == PspFixtureOutcome.FAULT_UNPROCESSED) {
				throw new PspUnprocessedFaultException();
			}
			decision = decisionFor(fixtureOutcome);
			decisionReason = decisionReasonFor(fixtureOutcome);
		}

		Instant now = clock.instant();

		try {
			repository.insert(
					request.merchantId(),
					request.pspOperationId(),
					request.paymentId(),
					request.amountMinor(),
					request.currency(),
					decision,
					decisionReason,
					"VOID",
					request.authorizationPspOperationId(),
					null,
					null,
					now);
		}
		catch (DataIntegrityViolationException exception) {
			PspOperationRecord raced = repository.findByMerchantAndPspOperationId(
							request.merchantId(), request.pspOperationId())
					.orElseThrow(() -> exception);
			return toVoidResult(resolveOrConflict(raced, request));
		}

		if (fixtureOutcome == PspFixtureOutcome.FAULT_COMMITTED_REPLY_DROPPED) {
			throw new PspReplyDroppedFaultException();
		}

		return new VoidOperationResult(
				new OperationDecisionResponse(request.pspOperationId(), decision, decisionReason, null, now),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	Optional<OperationDecisionResponse> findDecision(UUID pspOperationId) {
		return repository.findByPspOperationId(pspOperationId)
				.map(operation -> new OperationDecisionResponse(
						operation.pspOperationId(),
						operation.decision(),
						operation.decisionReason(),
						operation.validity(),
						operation.validityEvidenceAt()));
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, AuthorizeOperationRequest request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, CaptureOperationRequest request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, VoidOperationRequest request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private VoidOperationResult toVoidResult(PspOperationRecord record) {
		return new VoidOperationResult(
				new OperationDecisionResponse(
						record.pspOperationId(),
						record.decision(),
						record.decisionReason(),
						record.validity(),
						record.validityEvidenceAt()),
				record.decision().equals("PROCESSING"));
	}

	private CaptureOperationResult toCaptureResult(PspOperationRecord record) {
		return new CaptureOperationResult(
				new OperationDecisionResponse(
						record.pspOperationId(),
						record.decision(),
						record.decisionReason(),
						record.validity(),
						record.validityEvidenceAt()),
				record.decision().equals("PROCESSING"));
	}

	private AuthorizeOperationResult toResult(PspOperationRecord record) {
		return new AuthorizeOperationResult(
				new OperationDecisionResponse(
						record.pspOperationId(),
						record.decision(),
						record.decisionReason(),
						record.validity(),
						record.validityEvidenceAt()),
				record.decision().equals("PROCESSING"));
	}

	private String decisionFor(PspFixtureOutcome fixtureOutcome) {
		return switch (fixtureOutcome) {
			case APPROVED, APPROVED_EXPIRED_AUTHORIZATION, FAULT_COMMITTED_REPLY_DROPPED -> "SUCCEEDED";
			case DECLINED -> "REJECTED";
			case NON_EXECUTED -> "FAILED";
			case PROCESSING -> "PROCESSING";
			case FAULT_UNPROCESSED -> throw new IllegalStateException();
		};
	}

	private String decisionReasonFor(PspFixtureOutcome fixtureOutcome) {
		return switch (fixtureOutcome) {
			case DECLINED -> "CARD_DECLINED";
			case NON_EXECUTED -> "NON_EXECUTION_CONFIRMED";
			default -> null;
		};
	}

	private String validityFor(PspFixtureOutcome fixtureOutcome) {
		return fixtureOutcome == PspFixtureOutcome.APPROVED_EXPIRED_AUTHORIZATION ? "EXPIRED" : "ACTIVE";
	}
}
