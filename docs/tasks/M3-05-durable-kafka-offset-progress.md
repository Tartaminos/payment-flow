# M3-05 — Durable Manual Offset Progress

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Make consumer-group progress reflect only contiguous durably processed or independently handed-off Kafka records.

## Required Behavior

- Use manual commits of the next offset after the highest **contiguous eligible record per partition**, even when later workers finish first.
- Eligibility requires committed definitive state/audit, verified idempotent no-op or durable independent handoff; in-memory queueing and transient PSP replies never qualify.
- For UNKNOWN without independent recovery implementation, **do not commit its offset**. Prevent premature progress under crashes/rebalances; M5 technical retry and M6 reconciliation later expand handoffs.

## Scope Lock

**Include:** Partition-aware progress tracking, worker completion bookkeeping, safe reassignment and crash/ordering tests.

**Exclude:** No blanket consumer auto-commit, offset leaps over uncompleted work, speculative UNKNOWN acknowledgment or implementation of M5 retry topics.

## Authoritative Sources

- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [A20](../adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md)
- [A21](../adr/0021-small-mvp-kafka-partition-and-worker-topology.md)
- [A22](../adr/0022-bounded-per-payment-waiting-queue.md)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)

## Acceptance Criteria

1. If offset N is not durable but N+1 is complete, commit does **not** advance past N; once N is eligible, contiguous progress can move safely.
2. DB success then consumer crash/redelivery is idempotent; rebalance and queued-only work never skips a command.
3. UNKNOWN without independently durable reconciliation remains unacknowledged; tests explicitly demonstrate this boundary.

## Dependencies

- [M3-04](M3-04-bounded-payment-workers.md)
- M3 contract approval: offset ownership, rebalance/resume semantics and when an UNKNOWN recovery obligation is independent.
