package jhsoftware.pspsimulator;

import org.springframework.stereotype.Component;

@Component
class VoidFixtureOutcomeSelector {

	private static final long BAND_SIZE = 1000L;
	private static final long DECLINED_BAND_START = 300L;
	private static final long NON_EXECUTED_BAND_START = 350L;
	private static final long PROCESSING_BAND_START = 400L;
	private static final long FAULT_COMMITTED_REPLY_DROPPED_BAND_START = 450L;
	private static final long FAULT_UNPROCESSED_BAND_START = 480L;

	PspFixtureOutcome outcomeFor(long amountMinor) {
		long band = amountMinor % BAND_SIZE;
		if (band >= FAULT_UNPROCESSED_BAND_START) {
			return PspFixtureOutcome.FAULT_UNPROCESSED;
		}
		if (band >= FAULT_COMMITTED_REPLY_DROPPED_BAND_START) {
			return PspFixtureOutcome.FAULT_COMMITTED_REPLY_DROPPED;
		}
		if (band >= PROCESSING_BAND_START) {
			return PspFixtureOutcome.PROCESSING;
		}
		if (band >= NON_EXECUTED_BAND_START) {
			return PspFixtureOutcome.NON_EXECUTED;
		}
		if (band >= DECLINED_BAND_START) {
			return PspFixtureOutcome.DECLINED;
		}
		return PspFixtureOutcome.APPROVED;
	}
}
