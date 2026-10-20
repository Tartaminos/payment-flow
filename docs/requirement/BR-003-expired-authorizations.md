# BR-003 — Expired authorizations cannot be captured

- **Type:** Business rule
- **Status:** Accepted at the behavioral level; PSP contract details pending
- **Scope:** Card authorization lifecycle and capture endpoint
- **Related domain model:** [Payment state machines](../domain/payment-state-machines.md)

## Business context

A card authorization has a limited validity period. A capture request arriving after expiration must not generate an invalid or unsafe financial operation.

## Requirement

> PaymentFlow must not initiate capture against an authorization whose expiration has been reliably established. When capture is requested for such a payment, it must return a specific, actionable API error.

Distinguish the business outcomes:

- `EXPIRED`: the authorization validity window ended and expiration has been confirmed according to the PSP contract.
- `CANCELED`: a previously authorized payment was explicitly voided/canceled and the PSP confirmed the result.

Expiration is **not** automatically the same as cancellation. Do not issue an unnecessary `VOID` for an already expired authorization. An attempted void is a separate operation when the PSP still considers an authorization active.

## Expected behavior

1. Before capture, validate the current authorized state and the PSP-specific authorization validity.
2. If expiration is authoritative, do not send a capture to the PSP and transition `PaymentStatus.AUTHORIZED -> EXPIRED` safely.
3. A capture request against an expired authorization returns a domain error: proposed HTTP `409 Conflict` with `AUTHORIZATION_EXPIRED`.
4. If local expiry information and the PSP disagree or the outcome is uncertain, **do not assume** a financial outcome; resolve with the PSP before recording a definitive expiration.
5. Expiration and its evidence must be traceable for later audit.

## API example (proposed contract)

```http
HTTP/1.1 409 Conflict
Content-Type: application/json

{
  "error": "AUTHORIZATION_EXPIRED",
  "message": "Payment authorization has expired."
}
```

## Acceptance criteria

- **AC-001:** Given a confirmed expired authorization, a capture request produces no PSP capture operation.
- **AC-002:** Given a confirmed expired authorization, the payment is represented as `EXPIRED`, not `CANCELED`.
- **AC-003:** Given a capture attempt on an expired authorization, the API returns a distinct, documented error.
- **AC-004:** Given an already authorized payment with unconfirmed expiration, neither a timestamp alone nor a network timeout is treated as authoritative proof of the PSP's state.
- **AC-005:** Expiration cannot overwrite a confirmed `CAPTURED` payment.

## Open design decisions

- Which PSP source is authoritative for expiration and whether a query is required.
- Expiration time source, timezone, allowed clock skew, and race conditions between capture and expiry.
- Expiration detection by scheduler vs. capture pre-check.
- Whether the API returns the proposed HTTP `409` response shape exactly.
