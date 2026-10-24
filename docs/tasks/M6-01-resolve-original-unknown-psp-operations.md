# M6-01 — Resolve UNKNOWN by Original PSP Identity

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Safely reconcile an existing `UNKNOWN` financial operation using authoritative PSP evidence.

## Required Behavior

- Query the simulated PSP by the **original** operation/provider idempotency identity and correlate results with the existing PaymentOperation.
- Apply definitive authorization/capture/void evidence through permitted state transitions, with append-only audit and no new financial operation.
- On missing/ambiguous/conflicting evidence, retain `UNKNOWN` and the last confirmed Payment state; retry lookup separately from financial PSP attempts.

## Scope Lock

**Include:** Provider status-lookup integration, safe resolution transaction, idempotent late/duplicate evidence tests.

**Exclude:** No blind financial resubmission, automatic fail/cancel by timeout or accepting unauthenticated result events as authoritative.

## Authoritative Sources

- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [BR6](../requirement/BR-006-distinguish-technical-failure-from-decline.md)
- [BR7](../requirement/BR-007-bounded-psp-retries.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [STATES](../domain/payment-state-machines.md)

## Acceptance Criteria

1. Confirmed lookup resolves original operation and permitted Payment transition exactly once, correlated to audit.
2. Repeated late/duplicate lookup never creates another PaymentOperation or PSP financial effect.
3. Unavailable or ambiguous PSP status keeps `UNKNOWN` and last confirmed Payment status unchanged.

## Dependencies

- [M3-03](M3-03-bounded-psp-attempts-and-unknown.md)
- [M4-02](M4-02-execute-full-capture.md)
- [M5-04](M5-04-dispatch-due-technical-retries.md)
- M6 contract approval: PSP status-query evidence, reconciliation eligibility, polling/backoff and crash recovery.
