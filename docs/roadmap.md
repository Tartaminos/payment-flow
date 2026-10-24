# PaymentFlow — Implementation Roadmap

- **Status:** Roadmap established; M1 scope approved; M2–M6 are proposals for later approval.
- **Date:** 2026-10-24
- **Source of truth:** This file, the [M1 specification](milestones/m1-payment-creation-idempotency.md), [business requirements](requirement/), [domain](domain/) and [accepted ADRs](adr/).
- **Method:** Deliver small, reviewable increments. Codex implements only a milestone after its contracts and acceptance criteria are approved and committed. No downstream design gap authorizes inventing financial business rules.

## Milestones

| ID | Delivery | Status | Minimal demonstrable outcome |
| --- | --- | --- | --- |
| **M1** | **Payment Creation & Idempotency** | **Scope approved** | POST /payments persists Payment, initial AUTHORIZE PaymentOperation, idempotency association, audit and an Outbox publication intent in one transaction; returns durable acceptance, with tests. **No Kafka/PSP integration.** |
| **M2** | Outbox Publisher & Kafka Transport | Proposed | Committed Outbox intents are published at least once to the main Kafka command topic with agreed paymentId key and Avro/Schema Registry value. |
| **M3** | Payment Processor & Simulated Authorization | Proposed | One consumer decodes commands; bounded workers check DB state, call a fake PSP for AUTHORIZE, store definitive/unknown results and commit safe offsets. |
| **M4** | Capture, Void & Domain Invariants | Proposed | Admitted capture/cancel operations respect authorization, expiry, single-full-capture and conflicting-operation rules. |
| **M5** | Technical Retry, DLQ & Durable Scheduling | Proposed | Consumer technical errors have recoverable handoff to the single retry topic/DLQ; a persistent Retry Scheduler/Dispatcher redrives eligible work safely. |
| **M6** | PSP Uncertainty, Reconciliation & Observability | Proposed | Ambiguous outcomes are independently reconciled/escalated, and tests/diagnostics exercise failure recovery and tracing. |

This sequence is a **planning proposal**, not approval of the exact internals or endpoints of M2–M6. PSP webhooks, advanced operational tooling and any ledger remain separate scope choices, not implicit commitments.

## Approved M1 boundaries

The user explicitly approved **API + PostgreSQL + transactional Outbox + audit + automated tests, without Kafka in the first increment**. Its precise implementation requirements and open pre-coding contracts are tracked in [M1](milestones/m1-payment-creation-idempotency.md).

The API accepts requests durably; it does **not** report financial authorization/capture success before PSP processing exists. The Outbox is **persisted but not published** in M1.

## Suggested delivery rhythm

1. Agree on a milestone's scope.
2. Close only the contracts required for that milestone in GitHub (ADRs/specs where needed).
3. Commit a Codex-ready task brief with testable acceptance criteria and exclusions.
4. Implement in small pull requests, run tests and review against the source-of-truth docs.
5. Update documentation for explicitly approved changes; move to the next milestone after verification.

**Avoid speculative scaling:** local educational MVP, no production traffic target. When Kafka workers are implemented, the approved initial main-topic baseline is 4 partitions / 1 consumer instance / 4 async workers (ADR-0021). Backpressure queue sizes and detailed recovery are intentionally deferred until relevant.

## Design references

- [ADR-0004](adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md): single DB transaction for admission + Outbox.
- [BR-001](requirement/BR-001-idempotent-payment-requests.md): replay-safe payment creation.
- [Conceptual relational data model](domain/relational-data-model.md): four core domain tables; Outbox is additional supporting persistence.
- [ADR-0010](adr/0010-generic-execute-payment-operation-command.md): generic command for an already persisted PaymentOperation.
- [ADR-0020](adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md), [ADR-0021](adr/0021-small-mvp-kafka-partition-and-worker-topology.md), [ADR-0022](adr/0022-bounded-per-payment-waiting-queue.md): future worker/concurrency design.
