package jhsoftware.pspsimulator.service;

import jhsoftware.pspsimulator.dto.AuthorizeOperationRequestDto;
import jhsoftware.pspsimulator.dto.CaptureOperationRequestDto;
import jhsoftware.pspsimulator.dto.OperationDecisionResponseDto;
import jhsoftware.pspsimulator.dto.VoidOperationRequestDto;
import jhsoftware.pspsimulator.exception.PspOperationConflictException;
import jhsoftware.pspsimulator.exception.PspReplyDroppedFaultException;
import jhsoftware.pspsimulator.exception.PspUnprocessedFaultException;
import jhsoftware.pspsimulator.fixture.AmountMinorFixtureOutcomeSelector;
import jhsoftware.pspsimulator.fixture.CaptureFixtureOutcomeSelector;
import jhsoftware.pspsimulator.fixture.PspFixtureOutcome;
import jhsoftware.pspsimulator.fixture.VoidFixtureOutcomeSelector;
import jhsoftware.pspsimulator.model.AuthorizeOperationResult;
import jhsoftware.pspsimulator.model.CaptureOperationResult;
import jhsoftware.pspsimulator.model.PspOperationRecord;
import jhsoftware.pspsimulator.model.VoidOperationResult;
import jhsoftware.pspsimulator.repository.PspOperationRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
public class PspOperationService {

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

	public AuthorizeOperationResult authorize(AuthorizeOperationRequestDto request) {
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
				new OperationDecisionResponseDto(request.pspOperationId(), decision, decisionReason, validity, now),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	public CaptureOperationResult capture(CaptureOperationRequestDto request) {
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
				new OperationDecisionResponseDto(request.pspOperationId(), decision, decisionReason, null, now),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	public VoidOperationResult voidOperation(VoidOperationRequestDto request) {
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
				new OperationDecisionResponseDto(request.pspOperationId(), decision, decisionReason, null, now),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	public Optional<OperationDecisionResponseDto> findDecision(UUID pspOperationId) {
		return repository.findByPspOperationId(pspOperationId)
				.map(operation -> new OperationDecisionResponseDto(
						operation.pspOperationId(),
						operation.decision(),
						operation.decisionReason(),
						operation.validity(),
						operation.validityEvidenceAt()));
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, AuthorizeOperationRequestDto request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, CaptureOperationRequestDto request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, VoidOperationRequestDto request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private VoidOperationResult toVoidResult(PspOperationRecord record) {
		return new VoidOperationResult(
				new OperationDecisionResponseDto(
						record.pspOperationId(),
						record.decision(),
						record.decisionReason(),
						record.validity(),
						record.validityEvidenceAt()),
				record.decision().equals("PROCESSING"));
	}

	private CaptureOperationResult toCaptureResult(PspOperationRecord record) {
		return new CaptureOperationResult(
				new OperationDecisionResponseDto(
						record.pspOperationId(),
						record.decision(),
						record.decisionReason(),
						record.validity(),
						record.validityEvidenceAt()),
				record.decision().equals("PROCESSING"));
	}

	private AuthorizeOperationResult toResult(PspOperationRecord record) {
		return new AuthorizeOperationResult(
				new OperationDecisionResponseDto(
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
