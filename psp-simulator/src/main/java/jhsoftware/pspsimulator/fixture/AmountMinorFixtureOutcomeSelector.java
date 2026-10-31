package jhsoftware.pspsimulator.fixture;

import org.springframework.stereotype.Component;

@Component
public class AmountMinorFixtureOutcomeSelector {

	private static final long BAND_SIZE = 1000L;
	private static final long APPROVED_EXPIRED_AUTHORIZATION_BAND_START = 650L;
	private static final long DECLINED_BAND_START = 700L;
	private static final long NON_EXECUTED_BAND_START = 800L;
	private static final long PROCESSING_BAND_START = 850L;
	private static final long FAULT_COMMITTED_REPLY_DROPPED_BAND_START = 900L;
	private static final long FAULT_UNPROCESSED_BAND_START = 950L;

	public PspFixtureOutcome outcomeFor(long amountMinor) {
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
		if (band >= APPROVED_EXPIRED_AUTHORIZATION_BAND_START) {
			return PspFixtureOutcome.APPROVED_EXPIRED_AUTHORIZATION;
		}
		return PspFixtureOutcome.APPROVED;
	}
}
