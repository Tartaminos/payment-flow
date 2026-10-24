# BR-001 — Idempotent payment requests

- **Type:** Business rule
- **Status:** Business behavior agreed; **M1 creation implementation contract approved** on 2026-10-24 in [M1 contract](../contracts/m1-payment-creation-contract.md).
- **Scope:** Creation of a payment through the PaymentFlow API
- **Related:** [BR-002 — Uncertain PSP outcomes](BR-002-uncertain-psp-outcomes.md)

## Business context

An e-commerce client may retry a payment request because its connection timed out, the response was lost, or a user retried an action. A retry must not create another payment or initiate another charge for the **same logical request**.

## Requirement

> When PaymentFlow receives the same payment-creation request with the same `Idempotency-Key` and equivalent business parameters, it must resolve to the original logical payment, never to a second payment/financial operation.

### Expected behavior

1. A client initiates `POST /payments` with an `Idempotency-Key` and payment parameters.
2. PaymentFlow associates the key and request identity with **one** logical payment.
3. Repeating an equivalent request using that same key returns the already-known payment identity and its current outcome (including a pending/in-progress outcome when applicable), rather than creating another payment.
4. Reusing the same key **within the same scope** with different payment parameters is a conflict; reject it (M1 accepted HTTP status: `409 Conflict`).
5. Concurrent requests carrying the same key must not both create payments or each start independent downstream financial operations.

## Example

First attempt:

```http
POST /payments
Idempotency-Key: checkout-150-attempt-1
Content-Type: application/json

{
  "orderId": "150",
  "amount": 50000,
  "currency": "BRL"
}
```

A repeated request with identical business parameters and key must refer to the **same payment**, even if the original HTTP response was lost.

The amount is expressed in the currency's minor unit here: `50000` means BRL 500.00.

## Acceptance criteria

- **AC-001:** Given two equivalent requests using the same idempotency key, at most one logical payment is created.
- **AC-002:** Given the same key and different business parameters, the second request is rejected as a conflict and does not alter the original payment.
- **AC-003:** Given concurrent requests with the same key, at most one succeeds in creating the logical payment; the others must resolve safely to the original or a documented in-progress/conflict response.
- **AC-004:** Given an original payment whose processing is still pending, an equivalent HTTP retry must not start an independent financial operation.
- **AC-005:** A transport failure or a lost HTTP response does not invalidate the original key-to-payment association.

## Boundaries and open design decisions

**Established for M1:** [M1 contract](../contracts/m1-payment-creation-contract.md) approves a required `Idempotency-Key`, unique `(merchant_id, operation_scope=PAYMENT_CREATE, key)` scope, canonical SHA-256 of validated `orderId`, integer `amount` and uppercase `currency`, `202` replay of the original identity, `409` for changed parameters, PostgreSQL-backed concurrent first-request safety, and no automatic key expiry or purge in this local milestone.

**Not decided for future milestones:** a safe expiry/purge and replay-after-expiry policy, merchant authentication/multi-tenancy, provider-side financial idempotency, and any capture-specific refinements. Implementation placement/locking mechanics remain owned by the Architect while preserving the approved uniqueness and race semantics.

**Important:** API-level idempotency is not sufficient by itself to prevent a PSP from executing an external charge twice. See [BR-002](BR-002-uncertain-psp-outcomes.md).
