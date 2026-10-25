package jhsoftware.pspsimulator;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
class PspOperationService {

	private final PspOperationRepository repository;
	private final AmountMinorFixtureOutcomeSelector fixtureSelector;
	private final Clock clock;

	PspOperationService(PspOperationRepository repository, AmountMinorFixtureOutcomeSelector fixtureSelector, Clock clock) {
		this.repository = repository;
		this.fixtureSelector = fixtureSelector;
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

		try {
			repository.insert(
					request.merchantId(),
					request.pspOperationId(),
					request.paymentId(),
					request.amountMinor(),
					request.currency(),
					decision,
					decisionReason,
					clock.instant());
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
				new OperationDecisionResponse(request.pspOperationId(), decision, decisionReason),
				fixtureOutcome == PspFixtureOutcome.PROCESSING);
	}

	Optional<OperationDecisionResponse> findDecision(UUID pspOperationId) {
		return repository.findByPspOperationId(pspOperationId)
				.map(operation -> new OperationDecisionResponse(
						operation.pspOperationId(), operation.decision(), operation.decisionReason()));
	}

	private PspOperationRecord resolveOrConflict(PspOperationRecord existing, AuthorizeOperationRequest request) {
		if (!existing.matches(request)) {
			throw new PspOperationConflictException();
		}
		return existing;
	}

	private AuthorizeOperationResult toResult(PspOperationRecord record) {
		return new AuthorizeOperationResult(
				new OperationDecisionResponse(record.pspOperationId(), record.decision(), record.decisionReason()),
				record.decision().equals("PROCESSING"));
	}

	private String decisionFor(PspFixtureOutcome fixtureOutcome) {
		return switch (fixtureOutcome) {
			case APPROVED, FAULT_COMMITTED_REPLY_DROPPED -> "SUCCEEDED";
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
}
