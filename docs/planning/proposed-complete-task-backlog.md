# PaymentFlow — Proposed Complete Vertical-Slice Task Backlog

- **Status:** PLANNING PROPOSAL (2026-10-24). **M1-01 and M1-02 alone are approved**; M2–M6 and every task in those stages are **not yet approved**.
- **Scope:** Proposed implementation plan for the [existing six-milestone roadmap](../roadmap.md), not a change to the accepted ADRs.
- **Task policy:** One short, independently demonstrable vertical slice per Markdown file under `docs/tasks/`, with **Objective, Required Behavior, Scope Lock, Authoritative Sources, Acceptance Criteria, Dependencies**. The Architect determines internal Java design.
- **Do not create/execute the M2–M6 task Markdown files as approved requirements** until each milestone's scope and relevant contract decisions are accepted. This page is a planning inventory, not 19 Codex work orders.

## Proposed backlog: 21 slices total

### M1 — Payment Creation & Idempotency (2 approved)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M1-01 — Transactional Payment Acceptance](../tasks/M1-01-transactional-payment-acceptance.md) | `POST /payments` validates input and commits Payment, AUTHORIZE/PENDING operation, scoped idempotency, audit and a pending Outbox record atomically; returns `202` after commit. | Approved [M1 contract](../contracts/m1-payment-creation-contract.md) | **APPROVED** |
| [M1-02 — Idempotent Replay & Concurrency](../tasks/M1-02-idempotent-replay-and-concurrent-requests.md) | Equivalent replay returns the original Payment; conflicting key usage returns `409`; concurrent first requests create one logical payment. | M1-01 | **APPROVED** |

### M2 — Outbox Publisher & Kafka Transport (2 proposed)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| **M2-01 — First Outbox Command Published** | A committed `ExecutePaymentOperation` Outbox intent becomes a decodable Avro command in local Kafka using Schema Registry, `RecordNameStrategy`, `FULL_TRANSITIVE`, and the canonical `paymentId` UTF-8 key. Broker/registry in local Docker; command carries accepted identities, no real PSP processing. | M1-02; approved Kafka/Avro/topic contract | Proposed |
| **M2-02 — Reliable Outbox Recovery** | Kafka downtime and publisher crash leave pending work recoverable; retries can republish the **same logical messageId** at least once and mark delivery progress only after durable broker acknowledgment. Verify no payment disappears and duplicate publication remains possible/safe. | M2-01; approved claiming/retry contract | Proposed |

Relevant accepted sources: [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md), ADRs [0011](../adr/0011-execute-payment-operation-command-envelope.md)–[0019](../adr/0019-canonical-utf8-uuid-kafka-command-key.md). **Topic names, final Avro record/namespace, schema-ID handling, publisher claiming and publish recovery still need detailed agreement.**

### M3 — Payment Processor & Simulated Authorization (5 proposed)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| **M3-01 — Consume & Resolve Authorization** | A Kafka command drives the **existing** AUTHORIZE operation through a simulated PSP and durably records confirmed approval or decline plus append-only audit, without a new PaymentOperation on redelivery. The PSP call does not block Kafka polling. | M2-02; approved fake PSP/command processing contract | Proposed |
| **M3-02 — Read Payment Status** | A client can retrieve confirmed Payment state and relevant operation status via a **proposed** `GET /payments/{paymentId}`, including after asynchronous authorization. No inference of financial success from command delivery. | M3-01; **GET endpoint contract/scope approval** | Proposed |
| **M3-03 — Safe PSP Attempts & UNKNOWN** | Safe provider-idempotent calls use **at most 3 total attempts**, with **5/10/20 s** per-request timeouts. Definitive failures and declines differ; ambiguous outcomes remain `UNKNOWN` and do not reset the PSP budget after a Kafka replay. | M3-01; fake PSP idempotency/status contract | Proposed |
| **M3-04 — Bounded Parallel Processing** | Main-topic baseline **4 partitions / 1 Kafka consumer / 4 async workers**, global bounded intake and a bounded per-payment FIFO wait queue; at most one locally active financial operation per `paymentId`, plus DB safety across ownership changes. | M3-01, M3-03; agreed queue limits/backpressure | Proposed |
| **M3-05 — Durable Manual Offset Progress** | Commit only the **highest contiguous** offsets eligible after committed financial result or independently durable recovery handoff. Crash/rebalance/late-worker tests show no skipped operation; in-memory queue admission is never proof of durability. | M3-04; agreed tracking/rebalance and UNKNOWN recovery semantics | Proposed |

Sources: [ADR-0005](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0020](../adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md), [ADR-0021](../adr/0021-small-mvp-kafka-partition-and-worker-topology.md), [ADR-0022](../adr/0022-bounded-per-payment-waiting-queue.md), [BR-002](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-006](../requirement/BR-006-distinguish-technical-failure-from-decline.md), [BR-007](../requirement/BR-007-bounded-psp-retries.md).

**Sequencing caution:** M5's durable technical-retry handoff and M6's full reconciliation workflow are later. M3 **must not** prematurely commit an offset merely because PSP status is `UNKNOWN` or work was queued. A durable independently recoverable UNKNOWN path, or leaving offset uncommitted until one exists, needs an explicit M3 contract.

### M4 — Capture, Void & Domain Invariants (4 proposed)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| **M4-01 — Idempotent Full Capture Admission** | Proposed capture HTTP endpoint atomically admits one `CAPTURE:PENDING` and an Outbox intent for an eligible AUTHORIZED Payment; equivalent-key replay is stable; changed key/body, concurrent capture and unresolved/conflicting operations do not create another capture. | M3-05; capture API/idempotency contract | Proposed |
| **M4-02 — Execute Capture** | Generic worker consumes the admitted CAPTURE operation and records a **confirmed** full capture as `Payment:CAPTURED` / `CAPTURE:SUCCEEDED` with audit. Duplicate delivery cannot capture twice. | M4-01; fake PSP capture semantics | Proposed |
| **M4-03 — Void Authorized Payment** | Proposed void/cancel endpoint and generic worker execute an eligible VOID once, preserving original operation identity and yielding `CANCELED` **only on confirmed PSP void**. Block conflicting/unknown operations. | M4-02 (reuses generalized processing); void API/provider contract | Proposed |
| **M4-04 — Expired Authorization Guard** | Definitively expired authorizations become `EXPIRED` with audit and cannot be captured; provide a specific capture conflict. A timestamp, timeout or missing response **alone** never proves PSP expiry, and confirmed CAPTURED is never overwritten. | M4-01, M4-02; expiry-evidence contract | Proposed |

Sources: [BR-003](../requirement/BR-003-expired-authorizations.md), [BR-004](../requirement/BR-004-idempotent-capture-requests.md), [BR-008](../requirement/BR-008-block-conflicting-financial-operations.md), [state machines](../domain/payment-state-machines.md). **Capture/void endpoint shapes, provider evidence, expiry timing and error envelopes remain to be agreed.** Ordering of M4-04 relative to M4-02 can be refined once contracts are fixed.

### M5 — Technical Retry, DLQ & Durable Scheduling (4 proposed)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| **M5-01 — Durable Technical Retry Handoff** | Transient Kafka-processing/infrastructure failure transfers recoverable context to the **single retry topic** before source offset acknowledgment, without starting a new PSP financial operation. | M3-05; retry envelope/topic contract | Proposed |
| **M5-02 — Poison Message Quarantine** | Permanently unprocessable commands go to one DLQ with diagnostic identity and recovery context; the original offset advances only after **durably confirmed** quarantine. No false payment decline. | M5-01; poison-message taxonomy and DLQ contract | Proposed |
| **M5-03 — Persist Retry-Topic Schedules** | A retry-topic consumer creates exactly one durable schedule per logical retry identity; duplicates are idempotent and offset is committed only after PostgreSQL persistence. | M5-01; schedule schema/dedup contract | Proposed |
| **M5-04 — Dispatch Due Retries Safely** | The dispatcher durably republishes eligible work with **10 s × 2** exponential delays, **±20% jitter**, hard **5-minute cap**, crash/restart and concurrent claim safety; preserves `operationId` and correlation, and never resets the PSP attempt budget. | M5-03; retry publication/claim/recovery contract | Proposed |

Sources: [ADR-0006](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0007](../adr/0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008](../adr/0008-persistent-kafka-retry-scheduler.md), [ADR-0009](../adr/0009-exponential-backoff-with-jitter-for-kafka-retries.md). **No fixed retry-count-based DLQ for ongoing transient failures**; financial `UNKNOWN` takes the reconciliation path instead.

### M6 — PSP Reconciliation & Observability (4 proposed)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| **M6-01 — Resolve UNKNOWN by Original PSP Identity** | Safe PSP status lookup resolves the **original** UNKNOWN operation with authoritative evidence, changes Payment only for confirmed results and appends audit; repeated or late results cannot duplicate financial effects. | M3-03, M4-02, M5-04; PSP lookup/evidence contract | Proposed |
| **M6-02 — Escalate UNKNOWN After 24 Hours** | Persisted `UNKNOWN` work older than **24 hours** becomes discoverable/escalated for reconciliation, remains unresolved without evidence and does **not** become FAILED or CANCELED by age. Repeated escalation is idempotent. | M6-01; escalation/visibility contract | Proposed |
| **M6-03 — Traceability & Retention** | Correlated, sanitized HTTP/worker/PSP/Kafka diagnostics and meaningful metrics support following payment to audit and retry outcomes, while retaining **financial history/audit 5 years**, **technical logs 30 days** and **Kafka messages 7 days** under documented local policies. | M3–M5 behavior; logging/metrics/retention design | Proposed |
| **M6-04 — End-to-End Failure Recovery** | Automated integration scenarios prove recovery across lost HTTP replies, outbox duplicate publish, Kafka redelivery, consumer crash/rebalance, scheduler restart and ambiguous PSP results, without multiplying logical operations or claiming false success. | M6-01 through M6-03; agreed fault-injection/test acceptance | Proposed |

Sources: [BR-002](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-005](../requirement/BR-005-reconciliation-after-24h-unknown.md), [BR-006](../requirement/BR-006-distinguish-technical-failure-from-decline.md), [NFR-001](../requirement/NFR-001-transaction-traceability-and-retention.md), [ADR-0002](../adr/0002-append-only-audit-and-retention.md). No live financial credentials or real card data.

## Dependency chain and planning gates

**Default implementation sequence:** `M1-01 → M1-02 → M2-01 → M2-02 → M3-01 → M3-02 → M3-03 → M3-04 → M3-05 → M4-01 → M4-02 → M4-03 → M4-04 → M5-01 → M5-02 → M5-03 → M5-04 → M6-01 → M6-02 → M6-03 → M6-04`. Task dependencies above, not this total order, are authoritative after review.

**Before approving task Markdown files for M2–M6, decide contracts in this order:**

1. **M2 transport:** Kafka and Schema Registry local runtime; topic names and retention; Avro record namespace/name, `paymentId` in value or key only, serializer/schema validation and recovery/claiming behavior.
2. **M3 PSP and concurrency:** fake PSP contract (approved/declined/ambiguous/technical, safe idempotency, status query), GET status endpoint choice, bounded global/per-payment queue sizes and backpressure, manual contiguous offset-tracker and UNKNOWN durable recovery rules.
3. **M4 financial API:** capture/void HTTP contracts, scoped idempotency and operation admission, authoritative expiry confirmation and failures.
4. **M5 technical resilience:** retry/DLQ record contracts, durable transfer proof, scheduler deduplication/storage, dispatcher claim/lease and safe replay.
5. **M6 operational semantics:** PSP reconciliation evidence, 24-hour escalation representation, correlation/metrics/retention mechanics and failure-injection tests.

**Review gate:** This task count, order and M2–M6 additions are **only a proposed decomposition**. Once user-approved milestone-by-milestone, create its six-section `docs/tasks/Mx-nn-...` files with precise acceptance and sources, as was done for M1. Do not implement unapproved milestones or invent contracts in Codex.
