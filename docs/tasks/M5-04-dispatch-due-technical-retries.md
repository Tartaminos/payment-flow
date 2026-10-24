# M5-04 — Dispatch Due Technical Retries

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Redispatch eligible technical retry schedules through a crash-safe durable handoff.

## Required Behavior

- Calculate Kafka-processing delay as **10s base × 2 exponential growth**, jitter ±20%, hard 300s cap; prolonged transient problems remain recoverable rather than auto-DLQ.
- Claim eligible schedules safely and publish a new recoverable command (new `messageId` permitted, stable `operationId`/`correlationId` required) before recording durable dispatch progress.
- Duplicate dispatch after crash is safe through identity/DB guards; scheduling retries never increase or reset the original PSP financial HTTP budget.

## Scope Lock

**Include:** Due-work scan/claim, durable re-publication, backoff/jitter, scheduling and restart tests.

**Exclude:** No in-memory-only delayed retry, exactly-once publication claim, unapproved extra Kafka retry topic or new independent financial operation.

## Authoritative Sources

- [M5 proposed implementation contract](../contracts/m5-technical-retry-dlq-scheduler-contract.md) (**PROPOSED — review before implementation**)
- [A8](../adr/0008-persistent-kafka-retry-scheduler.md)
- [A9](../adr/0009-exponential-backoff-with-jitter-for-kafka-retries.md)
- [A7](../adr/0007-single-kafka-retry-topic-and-dlq.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [BR7](../requirement/BR-007-bounded-psp-retries.md)

## Acceptance Criteria

1. Due schedules dispatch no earlier than eligible time and obey jitter/cap with deterministic clock/random test control.
2. Crash after publish but before progress marking yields safe replay of the existing operation and no lost scheduled work.
3. Concurrent dispatcher claims do not silently lose schedules; prolonged transient failures do not automatically become DLQ/FAILED.

## Dependencies

- [M5-03](M5-03-persist-technical-retry-schedules.md)
- M5 contract approval: schedule ownership/claiming, dispatcher publication identity, durable handoff and cleanup policy.
