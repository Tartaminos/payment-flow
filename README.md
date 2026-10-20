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

Future system-design artifacts (functional/non-functional requirements, domain model, state transitions, high-level architecture, event contracts, failure analysis, and ADRs) will be added **as they are discussed and approved**.

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

Document the payment invariants, define a valid payment state machine, and examine failure modes **before** choosing the final service topology or writing production code.
