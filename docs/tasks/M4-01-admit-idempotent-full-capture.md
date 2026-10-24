# M4-01 — Idempotent Full Capture Admission

**Status:** APPROVED for implementation (2026-10-24). Execute through the local Codex orchestration skill **only after each dependency has passed independent verification**; M4 safe order is M4-01 → M4-04 → M4-02 → M4-03. Approval alone does not execute code.

## Objective

Admit one full capture operation through a replay-safe API without invoking the PSP in the HTTP request.

## Required Behavior

- For a verified eligible `AUTHORIZED` Payment, the approved capture endpoint atomically persists one `CAPTURE:PENDING`, scoped client idempotency, audit and a pending Outbox command referencing the operation. **Fail closed without affirmative PSP ACTIVE authorization evidence**; do not wait for M4-04 to make admission safe.
- Equivalent replay resolves to the same capture operation; changed parameters on a reused key conflict; a new key after `CAPTURED` returns `PAYMENT_ALREADY_CAPTURED`.
- Reject concurrent or independently conflicting capture admissions while a previous operation is `PENDING`, `PROCESSING` or `UNKNOWN` when business-incompatible.

## Scope Lock

**Include:** Full-capture admission API, concurrency-safe database constraints, idempotency, audit/Outbox and tests.

**Exclude:** No PSP capture execution (M4-02), partial/multiple capture, refunds or authenticated multi-merchant management.

## Authoritative Sources

- [M4 approved implementation contract](../contracts/m4-capture-void-expiration-contract.md) (**APPROVED**)
- [BR4](../requirement/BR-004-idempotent-capture-requests.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [STATES](../domain/payment-state-machines.md)
- [DMODEL](../domain/relational-data-model.md)

## Acceptance Criteria

1. First eligible capture yields one operation, audit and Outbox intent; **no PSP capture** or duplicate admission occurs. Expired/ambiguous PSP validity never admits a CAPTURE/Outbox, even before the M4-04 expiry-transition enhancement.
2. Same-key retry returns original identity; differing request/key conflicts are enforced transactionally, including concurrent requests.
3. Already CAPTURED or unresolved-conflicting operations cannot admit a second independent capture.

## Dependencies

- [M3-05](M3-05-durable-kafka-offset-progress.md)
- Approved [M4 implementation contract](../contracts/m4-capture-void-expiration-contract.md): capture endpoint request/response/errors, key scope, PSP identity and transaction eligibility.
