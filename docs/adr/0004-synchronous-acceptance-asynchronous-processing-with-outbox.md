# ADR-0004 — Synchronous Request Acceptance and Asynchronous Payment Processing with Transactional Outbox

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decision scope:** API processing boundary, reliable event publication, Kafka integration and PSP execution
- **Related:** [BR-001 — Idempotency](../requirement/BR-001-idempotent-payment-requests.md), [BR-002 — Uncertain PSP outcomes](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-004 — Capture idempotency](../requirement/BR-004-idempotent-capture-requests.md), [BR-007 — Bounded PSP HTTP attempts](../requirement/BR-007-bounded-psp-retries.md), [BR-008 — Conflicting operations](../requirement/BR-008-block-conflicting-financial-operations.md), [ADR-0002 — Audit history](0002-append-only-audit-and-retention.md), [Conceptual data model](../domain/relational-data-model.md)

## Context

PaymentFlow receives requests from an e-commerce client, records a payment, and then communicates with an external simulated Payment Service Provider (PSP). PSP calls can time out or be unavailable and follow an agreed sequence of up to three **safe** HTTP attempts with per-attempt timeout budgets of 5, 10 and 20 seconds.

The client must receive timely acknowledgment that PaymentFlow has accepted and **durably registered** a request, without waiting for the PSP's financial result. At the same time, payment creation and the intention to process it must not diverge when the application or Kafka is unavailable.

Writing to PostgreSQL and publishing directly to Kafka in separate steps is a **dual write**: a crash after a database commit but before successful publication can strand a payment.

## Decision

Use a **hybrid synchronous/asynchronous flow**:

1. **Synchronous admission and durability:** The Payment API receives, validates and checks the client request and its `Idempotency-Key`. It atomically persists the appropriate Payment/PaymentOperation state, idempotency association, append-only audit information, and an **Outbox event** in **one local database transaction**.
2. **Synchronous acknowledgment only after commit:** On successful commit, the API promptly acknowledges that the request was **accepted for processing**, including the durable Payment identity. **It does not claim authorization, capture or settlement succeeded.** The proposed HTTP response is `202 Accepted`; the final choice between `202` and alternatives such as `201 Created`, and the exact response payload, remain **API contract decisions**.
3. **Asynchronous event publication:** A separate logical **Outbox Publisher** reads committed, not-yet-confirmed Outbox entries and publishes them to **Apache Kafka**. An unavailable Kafka does not erase the committed payment or its publication intent; pending entries can be retried.
4. **Asynchronous financial execution:** A logical **Payment Processor** consumes Kafka messages, safely reserves the existing PaymentOperation under the concurrency/idempotency rules, and performs the permitted PSP HTTP action without keeping the original client HTTP request or a long database transaction open.
5. **Result recording:** A definitive, correlated PSP result is committed to PaymentOperation/Payment and the corresponding audit trail. If the PSP outcome is ambiguous, keep `PaymentOperation.status = UNKNOWN`, preserve the last confirmed Payment state and use the existing reconciliation requirements.
6. **Client status retrieval:** The client can query the Payment resource (proposed `GET /payments/{paymentId}`) to observe its **confirmed business status**, separately from in-flight/unknown operation state. Client notification webhooks may be considered later; they are **not** assumed by this decision.
7. **Capture:** The same synchronous-admission/asynchronous-execution principle applies to a proposed `POST /payments/{paymentId}/capture`, with capture-specific idempotency, authorization validity checks and the block on conflicting operations.

This ADR specifies **logical responsibilities**, not a mandatory microservices topology, independent deployments or exact Kafka topology. A single application with separately scheduled components is not excluded.

## Conceptual sequence

```mermaid
sequenceDiagram
    participant Client as E-commerce client
    participant API as Payment API
    participant DB as Transactional database
    participant Publisher as Outbox Publisher
    participant Kafka as Apache Kafka
    participant Worker as Payment Processor
    participant PSP as Simulated PSP

    Client->>API: POST /payments (Idempotency-Key)
    API->>DB: Begin transaction; payment + operation + idempotency + audit + outbox
    DB-->>API: Commit confirmed
    API-->>Client: Acknowledge accepted; paymentId (202 proposed)
    Publisher->>DB: Read committed pending outbox events
    Publisher->>Kafka: Publish operation event
    Kafka-->>Publisher: Broker acknowledgment
    Publisher->>DB: Mark outbox delivery progress
    Kafka->>Worker: Deliver event (possibly repeated)
    Worker->>DB: Atomically check/admit existing logical operation
    Worker->>PSP: HTTP action (same PSP operation identity when retry safe)
    PSP-->>Worker: Definitive result or ambiguous outcome
    Worker->>DB: Persist operation/payment outcome + audit atomically
    Client->>API: GET /payments/{paymentId}
    API->>DB: Read current confirmed state
    API-->>Client: Payment status and available operation state
```

The sequence illustrates responsibilities, **not** guaranteed message ordering or a promise that every PSP call returns a definitive answer.

## Invariants and failure handling

- A failed/rolled-back admission transaction must **not** receive a successful durable-acceptance acknowledgment or publish an independent financial-processing event.
- A successful commit means that the request and the Outbox publication intent are durable **even when Kafka is temporarily unavailable**.
- If the API commits but loses its HTTP response, an equivalent request with the same client `Idempotency-Key` resolves to the **existing** payment (BR-001), rather than creating another.
- The **Publisher can publish the same event more than once**: for example, Kafka confirms publication, but the Publisher crashes before recording that progress in the database. We therefore design for **at-least-once delivery**, not exactly-once end-to-end execution.
- Kafka redelivery or concurrent workers must not create a new logical PSP operation or repeat the financial effect. Admission control happens transactionally **before** the PSP request, followed by short-lived database transactions and provider-level idempotency when safe.
- A consumer crash **after** the PSP action but **before** persisting its result can leave an ambiguous outcome. Worker takeover cannot assume non-execution: check original operation identity and reconcile instead of blindly repeating the charge (BR-002 and BR-008).
- Operation `SUCCEEDED` and `REJECTED` are distinct PSP financial outcomes; `FAILED` requires authoritative evidence of technical non-execution; `UNKNOWN` covers ambiguous results ([ADR-0003](0003-distinguish-financial-success-from-rejection.md)).
- Audit history and Outbox messages have **different retention and purposes**. [NFR-001](../requirement/NFR-001-transaction-traceability-and-retention.md) retains durable financial audit records independently of Kafka's seven-day initial message retention.

## Alternatives considered

### A. Fully synchronous financial execution in the API request

**Rejected for the initial design.** Makes response latency and availability dependent on PSP processing and retry schedules; increases the risk of long-held client connections.

### B. Synchronous API write followed by direct Kafka publication

**Rejected.** This **dual write** can lose the publication intent on a crash between the database commit and Kafka publication.

### C. Synchronous durable acceptance plus Transactional Outbox and asynchronous PSP execution

**Accepted.** Uses one database transaction to register the business request and the intention to publish, while processing the external financial effect independently.

## Consequences and trade-offs

**Advantages**

- Prompt and accurate client acknowledgment, with no false promise of payment approval.
- Durable intent to process a committed payment despite temporary Kafka unavailability.
- Clear separation of incoming HTTP traffic and potentially slow/uncertain PSP operations.
- Concrete Kafka learning scenarios: duplicate deliveries, worker concurrency, outbox retries, consumer offset management and recovery.

**Costs / limitations**

- Additional Outbox storage, publisher lifecycle, monitoring, delivery lag and cleanup.
- Financial state becomes eventually consistent after initial API acknowledgment.
- The Outbox does **not** guarantee exactly-once Kafka delivery or exactly-once external PSP effects; idempotent consumers, provider guarantees and reconciliation remain essential.
- Availability/throughput bottlenecks and delayed processing must be surfaced to clients through status retrieval and eventual operational observability.

## Subsequent M1 implementation decisions

The [approved M1 API, idempotency and persistence contract](../contracts/m1-payment-creation-contract.md) (2026-10-24) resolves the **M1** acceptance status and response shape (`202`, `400`, `409`), SHA-256 canonical request identity, no automatic idempotency-key expiry in M1, and minimum pending Outbox persistence fields. The two [M1](../milestones/m1-payment-creation-idempotency.md) tasks were approved for local Codex implementation. The remaining questions below continue to apply outside the M1-specific decisions; neither the Kafka publisher nor PSP execution is part of M1.

## Open design questions (not approved by this ADR)

- Final acceptance HTTP response semantics (`202` vs. `201`), response body, status endpoint payload and error contracts.
- Exact `outbox_events` schema, publisher technique (polling vs. CDC), dispatch scheduling and failure/retry policies.
- Kafka topics, message envelopes, partition keys, ordering and consumer groups. **ADR-0010 subsequently accepts a generic [`ExecutePaymentOperation` command](0010-generic-execute-payment-operation-command.md) referencing the existing PaymentOperation; its exact serialized envelope and topic remain open.** **Offset commit semantics were subsequently accepted in [ADR-0005](0005-manual-kafka-offset-commit-and-recoverable-processing.md); [ADR-0006](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md) establishes controlled consumer retries, durable DLQ/quarantine for definitively unprocessable messages, and independent PSP reconciliation.** Concrete retry topology, DLQ contracts and redrive remain open.
- Worker reservation/lease design after process crashes, and the precise boundary between in-progress and unknown PSP outcomes.
- PSP callback/webhook inbox design, outbox publication of subsequent state changes, client notification webhooks.
- Queue/backpressure behavior when Kafka or the PSP is unavailable for extended periods.
- Idempotency-key retention and canonical request hashing.
- Deployment/service topology. This ADR is **not** an instruction to implement code or infrastructure.

## Documentation-only milestone

This document records an approved architecture decision. **No application code, SQL migrations, broker configuration or running infrastructure is created.**
