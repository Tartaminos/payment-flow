# M1-01 — Transactional Payment Acceptance

**Status:** APPROVED for implementation (2026-10-24). Execute via the configured local Codex orchestration skill; this file's creation/approval does not start execution.

## Objective

Deliver one working `POST /payments` vertical slice that durably accepts a new payment request.

## Required Behavior

- Validate the agreed request body and required `Idempotency-Key`; read the merchant from stable local configuration.
- Return `202` with persisted Payment/Operation identity and pending statuses **only after commit**.
- In one PostgreSQL transaction, create `Payment:CREATED`, `AUTHORIZE:PENDING`, scoped idempotency association, append-only admission audit and one `PENDING` Outbox command intent.
- Enforce DB uniqueness on scoped idempotency keys so a concurrent or repeated key cannot create another payment. Full safe replay/conflict responses are completed by M1-02.

## Scope Lock

**Include:** required Flyway schema, API endpoint, transactional persistence, meaningful validation/errors, stable configured merchant and integration tests against PostgreSQL.

**Exclude:** Kafka publication/broker, PSP calls, producer/consumer, `GET`/capture/void endpoints, retry execution, deployment of Java in Docker, and full duplicate-replay behavior (M1-02). **Do not release M1 after this slice alone.**

## Authoritative Sources

- [M1 API/persistence contract](../contracts/m1-payment-creation-contract.md) (**approved**)
- [M1 scope](../milestones/m1-payment-creation-idempotency.md)
- [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0023](../adr/0023-configured-single-merchant-for-educational-mvp.md)
- [Relational model](../domain/relational-data-model.md), [BR-001](../requirement/BR-001-idempotent-payment-requests.md), [ADR-0011](../adr/0011-execute-payment-operation-command-envelope.md)

## Acceptance Criteria

1. Valid first request returns the agreed `202` response and exactly the five linked durable records, including one unpublished Outbox intent.
2. Missing/invalid inputs receive safe documented errors without creating partial records.
3. A forced failure at any later insert rolls back the entire admission; no success acknowledgment is sent.
4. A duplicate scoped key cannot create a second Payment/Operation/Outbox entry, even under competing inserts; M1-02 supplies compliant replay/conflict responses.
5. App starts locally with PostgreSQL Docker and no Kafka broker or PSP; automated PostgreSQL-backed tests pass.

## Dependencies

- Approved [M1 contract](../contracts/m1-payment-creation-contract.md).
- Spring Initializr baseline already committed to `main`.
