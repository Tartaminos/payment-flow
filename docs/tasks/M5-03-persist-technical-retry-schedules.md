# M5-03 — Persist Retry-Topic Schedules

**Status:** APPROVED for implementation (2026-10-24). Execute via the local Codex orchestration skill **after the preceding dependency tasks are complete and independently verified**; a partial retry pipeline is not production/recovery complete. Approval alone does not run Codex.

## Objective

Durably record retry timing for commands received on the technical retry topic.

## Required Behavior

- Consume the one retry topic and atomically persist scheduling responsibility tied to a stable logical retry identity and the original operation.
- Commit the **retry-topic** offset only after successful durable schedule persistence; deduplicate broker redelivery.
- Keep delayed work recoverable after process restart and avoid changing PSP attempt counters or payment financial state.

## Scope Lock

**Include:** Persistent scheduled work, idempotent retry-topic ingestion and crash/replay integration tests.

**Exclude:** No in-memory-only scheduling, direct financial PSP retries or delivery before due-time (M5-04).

## Authoritative Sources

- [M5 approved implementation contract](../contracts/m5-technical-retry-dlq-scheduler-contract.md) (**APPROVED**)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [A7](../adr/0007-single-kafka-retry-topic-and-dlq.md)
- [A8](../adr/0008-persistent-kafka-retry-scheduler.md)
- [A9](../adr/0009-exponential-backoff-with-jitter-for-kafka-retries.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)

## Acceptance Criteria

1. One retried Kafka record delivered twice results in a single durable schedule and no duplicate financial operation.
2. Schedule storage failure leaves retry-topic offset uncommitted; restart restores accepted pending schedules.
3. Schedule stores enough source, correlation and operation identity to recover without optional Kafka tracing headers.

## Dependencies

- [M5-01](M5-01-durable-technical-retry-handoff.md)
- Approved [M5 implementation contract](../contracts/m5-technical-retry-dlq-scheduler-contract.md): schedule table, stable deduplication identity, schema retention and scheduling time basis.
