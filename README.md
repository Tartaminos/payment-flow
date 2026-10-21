# PaymentFlow

**PaymentFlow** is a learning-oriented, real-world-inspired **payment processing and orchestration platform**, designed around the reliability challenges of financial transactions and event-driven systems.

The project is being designed **system-design first**. Business requirements, invariants, failure scenarios, and architecture decisions are documented before implementing the Java services.

> **Status:** Design / requirements discovery. No payment processing capability has been implemented yet.

## Problem statement

An e-commerce application needs to initiate card payments through an external **Payment Service Provider (PSP)**. Network timeouts, retries, duplicated events, and partial failures must not lead to duplicate financial effects or lost transaction outcomes.

PaymentFlow will explore how to orchestrate this lifecycle while maintaining a traceable, recoverable source of truth.

## Intended scope

The initial case study focuses on **credit-card payments via a simulated PSP**, without handling real cardholder data or real money.

Planned capabilities to refine during requirements and system design:

- Accept payment requests and expose their current status.
- Authorize and capture payments through a simulated PSP.
- Receive PSP webhooks and recover from uncertain payment outcomes.
- Prevent duplicated payment creation and duplicated external financial effects.
- Explore asynchronous event processing, including Apache Kafka, with explicit failure-handling and delivery semantics.

Potential later extensions include reconciliation, a ledger, and notifications. **These are not commitments for the first implementation.**

## Engineering goals

- **Correctness:** model valid business transitions and financial invariants.
- **Idempotency:** make HTTP retries, consumer redelivery, and PSP retries safe.
- **Reliability:** recover from partial failures without silently losing payments.
- **Traceability:** correlate payment requests, operations, events, and PSP outcomes.
- **Pragmatic architecture:** introduce Kafka, services, and patterns when justified by requirements and measurable trade-offs.

Kafka is a transport/event-log technology, **not an automatic guarantee that an external payment will be executed exactly once**.

## Documentation

Documentation is the source of truth for decisions made during the system-design exercise.

### Agreed business requirements

| ID | Requirement |
| --- | --- |
| [BR-001](docs/requirement/BR-001-idempotent-payment-requests.md) | Repeated equivalent payment requests must not create a second financial operation. |
| [BR-002](docs/requirement/BR-002-uncertain-psp-outcomes.md) | An uncertain PSP outcome must not be treated as failure or blindly retried as a new operation. |
| [BR-003](docs/requirement/BR-003-expired-authorizations.md) | Confirmed expired authorizations cannot be captured. |
| [BR-004](docs/requirement/BR-004-idempotent-capture-requests.md) | Capture retries are idempotent and new captures after a completed capture are rejected. |
| [BR-005](docs/requirement/BR-005-reconciliation-after-24h-unknown.md) | An operation still UNKNOWN after 24 hours requires reconciliation, not automatic cancellation. |
| [BR-006](docs/requirement/BR-006-distinguish-technical-failure-from-decline.md) | Definitive technical failure, authorization decline, and uncertain outcome must remain distinct. |
| [BR-007](docs/requirement/BR-007-bounded-psp-retries.md) | Bounded, PSP-safe HTTP attempts with per-attempt timeouts of **5s → 10s → 20s**; exhaustion alone never proves failure. |
| [BR-008](docs/requirement/BR-008-block-conflicting-financial-operations.md) | Block new conflicting operations while prior PSP outcomes remain unresolved; no override in the MVP. |

### Non-functional requirements

- [NFR-001 — Transaction Traceability and Audit Retention](docs/requirement/NFR-001-transaction-traceability-and-retention.md) — durable payment audit trail; **5 years** financial/audit history, **30 days** technical logs, **7 days** Kafka messages.

### Domain design and decisions

- [Payment and PaymentOperation state machines](docs/domain/payment-state-machines.md) — approved business and operation states, including `REJECTED`, and their primary transitions.
- [Relational data model](docs/domain/relational-data-model.md) — four approved conceptual tables, fields, relationships, and cardinality, with implementation choices clearly identified.
- [ADR-0001 — Separate Payment from PaymentOperation](docs/adr/0001-separate-payment-and-payment-operations.md) — rationale, rejected alternatives, and consequences.
- [ADR-0002 — Append-only audit and retention](docs/adr/0002-append-only-audit-and-retention.md) — database audit history, rationale and retention boundaries.
- [ADR-0003 — Separate operation success from rejection](docs/adr/0003-distinguish-financial-success-from-rejection.md) — why `REJECTED` is distinct from `SUCCEEDED`, `FAILED`, and `UNKNOWN`.
- [ADR-0004 — Synchronous acceptance, asynchronous PSP processing](docs/adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md) — synchronous durable API acknowledgment, Transactional Outbox, Kafka delivery and asynchronous PSP execution.
- [ADR-0005 — Manual Kafka offset commit and recoverable processing](docs/adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md) — commit consumption progress only after a durable outcome or recoverable handoff, never solely after an in-memory PSP result.
- [ADR-0006 — Consumer retries, poison-message quarantine and financial reconciliation](docs/adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md) — transient infrastructure failures use controlled retries, unprocessable Kafka messages use durable quarantine/DLQ, and uncertain PSP outcomes use reconciliation.
- [ADR-0007 — Single Kafka retry topic and DLQ](docs/adr/0007-single-kafka-retry-topic-and-dlq.md) — one retry destination and one quarantine destination for consumer failures; durable forwarding before source offset acknowledgment and DB-guarded financial operations.

Future system-design artifacts (functional/non-functional requirements, high-level architecture, event contracts, failure analysis, and further ADRs) will be added **as they are discussed and approved**.

## Proposed technical direction (not yet an implementation decision)

- **Language:** Java
- **Framework:** Spring Boot
- **Event platform to study:** Apache Kafka
- **Transactional data store candidate:** PostgreSQL
- **Local development candidate:** Docker Compose
- **Testing candidates:** JUnit and Testcontainers

Exact versions, service boundaries, event contracts, and persistence patterns remain open for evaluation.

## Safety boundary

All payment credentials, card tokens, and PSP interactions in this repository must be synthetic. No real PAN/CVV data, real payment credentials, or live financial transactions should be used in examples or tests.

## Current milestone

BR-001 through BR-008 and NFR-001 are documented. Payment/PaymentOperation state machines, append-only audit retention, bounded PSP HTTP attempt timeouts (5s/10s/20s), and blocking of conflicting unresolved operations are agreed. The conceptual four-table relational model and PSP outcome semantics are documented. **ADR-0004 approves synchronous durable request acceptance and asynchronous PSP execution via Transactional Outbox and Kafka.** ADR-0005 establishes manual offset commits after durable processing or independently recoverable handoff. ADR-0006 separates transient consumer retries, permanent-message quarantine and uncertain PSP reconciliation. ADR-0007 accepts one Kafka consumer retry topic and one DLQ topic, with durable handoff and database-enforced financial safety. Next, design the actual retry delay/backoff and scheduling approach, plus Kafka event contracts, the main-topic key/partitioning and consumer groups, worker recovery and outstanding API/database details. No implementation code is authorized at this documentation-only stage.
