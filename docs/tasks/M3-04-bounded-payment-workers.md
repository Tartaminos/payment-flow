# M3-04 — Bounded Parallel Payment Processing

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Process different payments concurrently without overlapping conflicting operations for one payment.

## Required Behavior

- Honor the accepted **4 Kafka partitions / 1 active consumer / 4 async workers** small-MVP baseline; bounds apply to active work and all queued work.
- Locally busy payments wait in a bounded per-payment FIFO queue; do not auto-route them to Kafka technical retry merely because a worker is busy.
- Use backpressure rather than dropping/unbounded buffering; before executing queued work, recheck DB state. Local exclusion does not replace database concurrency controls.

## Scope Lock

**Include:** Worker dispatch/queues/backpressure, state revalidation and parallelism/rebalance tests.

**Exclude:** No automatic offset acknowledgment on enqueue, no production throughput SLA, no multi-instance guarantee based on in-memory locks.

## Authoritative Sources

- [A20](../adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md)
- [A21](../adr/0021-small-mvp-kafka-partition-and-worker-topology.md)
- [A22](../adr/0022-bounded-per-payment-waiting-queue.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)

## Acceptance Criteria

1. Independent payments can execute concurrently up to four active workers, while one payment has no simultaneous conflicting PSP operations.
2. Limits trigger controlled backpressure without losing already-polled records; queues are bounded and FIFO per payment.
3. Crash/rebalance of queued or active work cannot cause in-memory completion to be treated as durable Kafka progress.

## Dependencies

- [M3-01](M3-01-process-simulated-authorization.md)
- [M3-03](M3-03-bounded-psp-attempts-and-unknown.md)
- M3 contract approval: numeric per-payment/global limits, fairness, poll/pause/resume and rebalance safety.
