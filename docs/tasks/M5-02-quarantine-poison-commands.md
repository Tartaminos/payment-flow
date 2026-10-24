# M5-02 — Poison Command Quarantine

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Quarantine definitively unprocessable commands without erasing or fabricating financial outcomes.

## Required Behavior

- Classify malformed/incompatible/unsupported command records as poison using a defined taxonomy and persist original diagnostic identity.
- Durably send poison data and failure context to the single DLQ **before** source-offset acknowledgment; permit controlled later investigation/replay of existing operation.
- Do not treat infrastructure transients or uncertain PSP effects as poison automatically.

## Scope Lock

**Include:** DLQ path, diagnostics and integration tests for invalid Avro/command inputs and DLQ failure.

**Exclude:** No silent record drops, automatic PSP failure transitions, automatic replay without state checks or retry-count-based poison classification.

## Authoritative Sources

- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [A6](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md)
- [A7](../adr/0007-single-kafka-retry-topic-and-dlq.md)
- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [BR6](../requirement/BR-006-distinguish-technical-failure-from-decline.md)

## Acceptance Criteria

1. Invalid command is recoverably quarantined with source coordinates and reason; no financial status is invented.
2. DLQ publication failure leaves original offset uncommitted.
3. Transient infrastructure failure and UNKNOWN PSP outcome are not routed to DLQ as definitive poison.

## Dependencies

- [M5-01](M5-01-durable-technical-retry-handoff.md)
- M5 contract approval: poison taxonomy, quarantine record/schema and controlled replay policy.
