# BR-004 — Capture requests must be idempotent and reject new captures after capture

- **Type:** Business rule
- **Status:** Accepted for MVP single full capture; implementation details pending
- **Scope:** Card capture endpoint and PSP capture operations
- **Related:** [BR-001](BR-001-idempotent-payment-requests.md), [BR-002](BR-002-uncertain-psp-outcomes.md), [BR-008](BR-008-block-conflicting-financial-operations.md)

## Business context

A client may retry a capture request after losing its HTTP response. It must not create another financial action. A *new* capture request against an already captured payment must also not be sent to the PSP.

## Requirement

> Replaying the same capture operation with the same idempotency identity must resolve to the original result. A distinct capture request for a fully captured payment must be rejected without charging again.

For this MVP, **only one full capture per authorization** is supported. Partial or multiple captures are out of scope.

## Expected behavior

1. Each capture request has an idempotency identity scoped to the appropriate client/merchant, payment, and action (exact scope to be designed).
2. Repeating the same capture request with the **same key and equivalent parameters** returns the previously recorded operation/payment outcome or an in-progress representation. No second PSP financial action is issued.
3. Reusing the same key with different parameters is rejected as a conflict.
4. Sending a **different key** to capture an already `CAPTURED` payment returns a specific conflict (`PAYMENT_ALREADY_CAPTURED`); it is not silently reported as a successful new capture.
5. Concurrent capture attempts must be serialized or rejected through transactional/state/uniqueness controls; a status pre-check alone is insufficient.
6. If a previous capture operation is `UNKNOWN`, a new independently identified capture must not begin until the original outcome is safely resolved (see [BR-002](BR-002-uncertain-psp-outcomes.md) and [BR-008](BR-008-block-conflicting-financial-operations.md)).

## Example response to a different key (proposed)

```http
HTTP/1.1 409 Conflict
Content-Type: application/json

{
  "error": "PAYMENT_ALREADY_CAPTURED",
  "message": "This payment has already been captured."
}
```

## Acceptance criteria

- **AC-001:** The same key and same capture parameters never generate an additional PSP capture.
- **AC-002:** Retry of an already completed capture resolves to the existing outcome.
- **AC-003:** A new key against an already captured payment is rejected without contacting the PSP to capture.
- **AC-004:** Reusing a capture key with different parameters is rejected as a conflict.
- **AC-005:** Concurrent capture requests cannot cause more than one full capture.
- **AC-006:** An unresolved capture operation blocks a new independent capture.

## Open design decisions

- Idempotency key scope and retention for capture requests.
- Exact HTTP status and payload for replayed in-progress operations.
- Transactional locking vs. conditional updates, uniqueness constraints, and PSP idempotency contract.
