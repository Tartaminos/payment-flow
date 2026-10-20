# ADR-0001 — Separate Payment from PaymentOperation

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decision scope:** Domain model / payment lifecycle
- **Reference:** [Payment and Payment Operation State Machines](../domain/payment-state-machines.md)

## Context

PaymentFlow orchestrates credit-card payments via an external PSP. It must handle retries, timeouts, duplicate delivery, and partial failures without inferring financial outcomes from transport errors.

An initial single-status proposal included `CREATED`, `PROCESSING`, `AUTHORIZED`, `CAPTURED`, `DECLINED`, `PENDING_CONFIRMATION`, `CONFIRMED`, and `DONE`.

However:

- `CONFIRMED` and `DONE` are ambiguous: authorized, captured, and settled are different facts.
- `PROCESSING` does not identify whether an authorization or a capture is running.
- `PENDING_CONFIRMATION` on the payment obscures which external operation has an unknown result.
- A payment may be **authorized** while its **capture outcome is unknown**. These two facts must coexist.

## Decision

Model **Payment** separately from **PaymentOperation**, with independent state machines.

- `Payment.status` uses the MVP business states: `CREATED`, `AUTHORIZED`, `CAPTURED`, `DECLINED`, `CANCELED`.
- A `PaymentOperation` has its own identity, references its Payment, and carries `type` (`AUTHORIZE`, `CAPTURE`, `VOID`) and `status`.
- `PaymentOperation.status` uses: `PENDING`, `PROCESSING`, `SUCCEEDED`, `FAILED`, `UNKNOWN`.
- `PaymentOperationStatus` is a value/enum, not a third entity.
- In particular, PSP timeouts / uncertain results are recorded as `PaymentOperationStatus.UNKNOWN`; they do not override the last confirmed `Payment.status`.

Authorization/capture business results must be kept distinct from whether the corresponding PSP interaction has a definitive execution result. Their final data representation is deferred.

See the state-machine document for primary transitions, examples, and open questions.

## Alternatives considered

### A. One Payment.status enum for every condition

**Rejected.** Combines financial facts with in-flight technical operations and loses context for authorization vs. capture timeouts.

### B. Payment + PaymentOperation with independent status

**Accepted.** Preserves the last confirmed financial state while separately tracking each external action and its recoverability.

## Consequences

**Benefits**
- Independent reconciliation and correlation of authorization, capture, and void.
- A known payment state remains visible even when another operation outcome is unknown.
- Clear boundaries for idempotency keys, retries, and PSP callbacks.
- More precise failure scenarios, tests, and audit data.

**Costs / trade-offs**
- Additional domain object and relationship to maintain.
- State updates require consistent transactional rules and concurrency control.
- Consumers and APIs may need a composed view of Payment and in-flight operations.
- This separation does not, on its own, solve external exactly-once processing or authorize a particular microservice topology.

## Explicit non-decisions

This ADR does **not** select a database schema, service boundaries, Kafka topics, message contracts, retry algorithms, partial capture support, event sourcing, or the PSP integration contract. Those require separate requirements/ADRs.
