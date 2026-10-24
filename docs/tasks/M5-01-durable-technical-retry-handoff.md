# M5-01 — Durable Technical Retry Handoff

**Status:** APPROVED for implementation (2026-10-24). Execute via the local Codex orchestration skill **after the preceding dependency tasks are complete and independently verified**; a partial retry pipeline is not production/recovery complete. Approval alone does not run Codex.

## Objective

Transfer transient Kafka-consumer processing failures to the one approved technical retry topic.

## Required Behavior

- Classify transient infrastructure/consumer failures separately from poison commands and ambiguous PSP financial outcomes.
- Publish recoverable retry context with source coordinates and original stable PaymentOperation identity; verify durable Kafka acknowledgment **before** committing the source offset.
- On handoff failure, keep source offset uncommitted; repeated handoff is safe under at-least-once delivery and cannot reset a PSP attempt budget.

## Scope Lock

**Include:** Main-topic retry handoff, recovery context and injected broker/database-failure tests.

**Exclude:** No PSP financial retry disguised as technical retry, no fixed retry-count-to-DLQ policy or delayed dispatcher (M5-04). **Do not claim M5's retry path is deployable/recoverable beyond Kafka retention until M5-03's durable scheduler ingestion exists**; the retry-topic is only a time-limited durable buffer.

## Authoritative Sources

- [M5 approved implementation contract](../contracts/m5-technical-retry-dlq-scheduler-contract.md) (**APPROVED**)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [A6](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md)
- [A7](../adr/0007-single-kafka-retry-topic-and-dlq.md)
- [A8](../adr/0008-persistent-kafka-retry-scheduler.md)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)

## Acceptance Criteria

1. Transient failure with acknowledged retry handoff makes source progress eligible; retry topic retains operation/source identity.
2. Retry publish failure does not advance source offset or lose recoverable work.
3. Duplicate handoff cannot create a new PaymentOperation or additional independent PSP action.

## Dependencies

- [M3-05](M3-05-durable-kafka-offset-progress.md)
- Approved [M5 implementation contract](../contracts/m5-technical-retry-dlq-scheduler-contract.md): retry topic, payload/envelope, handoff durability and retry identity.
