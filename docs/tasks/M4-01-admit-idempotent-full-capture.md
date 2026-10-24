# M4-01 — Idempotent Full Capture Admission

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Admit one full capture operation through a replay-safe API without invoking the PSP in the HTTP request.

## Required Behavior

- For a verified eligible `AUTHORIZED` Payment, a proposed capture endpoint atomically persists one `CAPTURE:PENDING`, scoped client idempotency, audit and a pending Outbox command referencing the operation.
- Equivalent replay resolves to the same capture operation; changed parameters on a reused key conflict; a new key after `CAPTURED` returns `PAYMENT_ALREADY_CAPTURED`.
- Reject concurrent or independently conflicting capture admissions while a previous operation is `PENDING`, `PROCESSING` or `UNKNOWN` when business-incompatible.

## Scope Lock

**Include:** Full-capture admission API, concurrency-safe database constraints, idempotency, audit/Outbox and tests.

**Exclude:** No PSP capture execution (M4-02), partial/multiple capture, refunds or authenticated multi-merchant management.

## Authoritative Sources

- [BR4](../requirement/BR-004-idempotent-capture-requests.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [STATES](../domain/payment-state-machines.md)
- [DMODEL](../domain/relational-data-model.md)

## Acceptance Criteria

1. First eligible capture yields one operation, audit and Outbox intent; neither a PSP call nor duplicate admission occurs.
2. Same-key retry returns original identity; differing request/key conflicts are enforced transactionally, including concurrent requests.
3. Already CAPTURED or unresolved-conflicting operations cannot admit a second independent capture.

## Dependencies

- [M3-05](M3-05-durable-kafka-offset-progress.md)
- M4 contract approval: capture endpoint request/response/errors, key scope, PSP identity and transaction eligibility.
