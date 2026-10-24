# M6-04 — End-to-End Failure Recovery

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Demonstrate that PaymentFlow recovers safely from realistic failures across the complete approved workflow.

## Required Behavior

- Exercise lost initial HTTP response, duplicate/out-of-order command, Kafka/broker downtime, publisher crash, worker crash/rebalance, retry scheduler restart, ambiguous PSP response and authorized/captured/voided/expired payment paths.
- Verify PostgreSQL Payment/PaymentOperation/audit remains authoritative throughout, independently recoverable work is not lost, and confirmed financial effects are never duplicated.
- Failures may produce delayed/UNKNOWN results, but never fabricated success, failed/canceled-by-timeout or skipped offsets.

## Scope Lock

**Include:** Automated Docker-backed failure-injection/integration suites, deterministic cases and concise reproducible run instructions.

**Exclude:** No live PSP/real card credentials, unapproved performance SLA or guarantee of distributed exactly-once external charges.

## Authoritative Sources

- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [BR5](../requirement/BR-005-reconciliation-after-24h-unknown.md)
- [BR6](../requirement/BR-006-distinguish-technical-failure-from-decline.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)
- [NFR1](../requirement/NFR-001-transaction-traceability-and-retention.md)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [A6](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md)
- [A8](../adr/0008-persistent-kafka-retry-scheduler.md)

## Acceptance Criteria

1. Automated authorization, capture, void and confirmed-expiry failure scenarios prove eventual safe recovery or explicitly visible unresolved work with the original financial identity.
2. Duplicate delivery, reruns and crashes do not create a second logical operation or silently skip a not-yet-durable Kafka record.
3. Traces/audit support investigation without secrets; test suite can run locally against PostgreSQL/Kafka/Schema Registry/test PSP.

## Dependencies

- [M4-04](M4-04-guard-expired-authorizations.md)
- [M6-01](M6-01-resolve-original-unknown-psp-operations.md)
- [M6-02](M6-02-escalate-unknown-after-24h.md)
- [M6-03](M6-03-traceability-and-retention.md)
- M6 contract approval: test environment/fault-injection matrix and acceptable terminal or visible-UNKNOWN outcomes.
