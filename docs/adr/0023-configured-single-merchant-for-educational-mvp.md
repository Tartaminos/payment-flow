# ADR-0023 — Single Configured Merchant for the Educational MVP

- **Status:** Accepted
- **Date:** 2026-10-24
- **Scope:** Merchant identity at the Payment Creation API boundary for the first local MVP increment
- **Related:** [BR-001 — Idempotent payment creation](../requirement/BR-001-idempotent-payment-requests.md), [ADR-0004 — Transactional request acceptance](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [Conceptual relational data model](../domain/relational-data-model.md), [M1](../milestones/m1-payment-creation-idempotency.md)

## Context

The conceptual data model already includes a required `payments.merchant_id` and idempotency-key uniqueness scoped by `(merchant_id, operation_scope, key)`. We considered receiving a synthetic merchant identifier through an untrusted HTTP header versus a **single known merchant** from local application configuration.

PaymentFlow is an educational local MVP with no authentication/merchant registry in the first increment. Accepting a client-supplied merchant header would add a concept we cannot authenticate and is unnecessary for the current learning objective.

## Decision

**Use one stable, synthetic merchant UUID read from application configuration for the first MVP. Do not accept a client-supplied `X-Merchant-Id` header or merchant field as the source of merchant identity.**

1. The API resolves merchant identity from the locally configured value for all requests in the first increment, rather than trusting unverified client claims.
2. Persist this `merchant_id` in `Payment` and the scoped client-idempotency association, preserving the approved database shape and `(merchant_id, operation_scope, key)` uniqueness design. **Do not collapse the database uniqueness contract to key alone** merely because one merchant is configured now.
3. Merchant identity must remain **stable across restarts** and not be randomly generated per request or per process start. The specific example UUID and configuration property name are implementation details to specify before Codex coding.
4. No merchant-management table, merchant-switching endpoint, authentication/authorization layer or dynamic merchant selection is added to M1.
5. This is explicitly a local **single-tenant teaching shortcut**, not a production authorization model. If multi-merchant support is introduced later, identity should be derived from trusted authentication context and documented in a new decision.

## Alternatives

- Client-supplied `X-Merchant-Id`: **not chosen** because its identity would be unauthenticated and unnecessary for M1.
- Single stable configured merchant: **accepted** for educational simplicity.

## Open implementation details

Configuration property name, synthetic UUID to use in local setup, error behavior if it is missing or malformed, exact request/response payload and remaining idempotency/physical storage choices.

## Documentation-only boundary

No runtime configuration, security code, Java files or database migrations are created by this ADR.
