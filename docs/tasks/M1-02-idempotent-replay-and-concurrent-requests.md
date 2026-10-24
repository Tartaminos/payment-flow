# M1-02 — Idempotent Payment Replay and Concurrency

**Status:** APPROVED for implementation (2026-10-24). Execute after M1-01 via the configured local Codex orchestration skill; this file's creation/approval does not start execution.

## Objective

Complete `POST /payments` with safe idempotent retries, conflicting-request rejection and concurrent first-request behavior.

## Required Behavior

- Resolve a repeated `Idempotency-Key` within the agreed merchant/`PAYMENT_CREATE` scope using the canonical validated business parameters.
- Same key and same request → `202`, **same persisted paymentId and operationId**, current persisted statuses; no additional admission, audit or Outbox.
- Same key and different request → `409 IDEMPOTENCY_KEY_CONFLICT`, without changes to the existing Payment.
- When two matching requests race, exactly one wins admission durably; the other resolves to that result when committed. No `202` may depend on only an uncommitted in-memory reservation.

## Scope Lock

**Include:** replay/concurrency logic for the existing endpoint, DB-backed atomic coordination/unique-conflict handling and independent integration/concurrency regression tests.

**Exclude:** new endpoints, authentication, key expiry/purge, Kafka/PSP activity, retry scheduler or changes to financial state semantics. Reuse M1-01 records; do not introduce a second payment creation path.

## Authoritative Sources

- [M1 API/persistence contract](../contracts/m1-payment-creation-contract.md) (**approved**)
- [M1 scope](../milestones/m1-payment-creation-idempotency.md)
- [BR-001](../requirement/BR-001-idempotent-payment-requests.md), [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0023](../adr/0023-configured-single-merchant-for-educational-mvp.md)
- [Relational model](../domain/relational-data-model.md)

## Acceptance Criteria

1. Same key/equivalent business request, including different JSON field order, returns the original identity and does not add any operation/audit/Outbox record.
2. Same key/different validated amount, currency or order ID returns `409` and preserves the original records.
3. Concurrent same-key requests create **exactly one** durable payment/operation/audit/Outbox set and resolve consistently to one winner; no duplicate external side effects.
4. Lost-response retry succeeds with original IDs after application restart; key remains recognized without an automatic expiry policy.
5. PostgreSQL-backed tests prove the failure/concurrency paths; M1 acceptance criteria are satisfied together with M1-01.

## Dependencies

- [M1-01 — Transactional Payment Acceptance](M1-01-transactional-payment-acceptance.md).
- Approved [M1 contract](../contracts/m1-payment-creation-contract.md).
