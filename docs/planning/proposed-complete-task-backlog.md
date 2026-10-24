# PaymentFlow — Complete Vertical-Slice Task Backlog

- **Status:** **21-task vertical-slice breakdown APPROVED (2026-10-24)**. **All 21 tasks and all six milestone contracts are APPROVED for sequential Codex implementation** (2026-10-24), each gated by completion and verification of its prerequisites.
- **Scope:** Approved task decomposition against the [six-milestone roadmap](../roadmap.md), with explicitly approved M1–M6 milestone internals and contracts.
- **Review:** [Full 21-task consistency review](task-backlog-review.md) — six-section structure, links, dependency graph and cross-milestone risk gates. The [complete M3–M6 decision review](m3-m6-contract-decision-review.md) records the subsequently **approved** M3–M6 contracts and their safety invariants.
- **Task policy:** One short, independently demonstrable vertical slice per Markdown file under `docs/tasks/`, with **Objective, Required Behavior, Scope Lock, Authoritative Sources, Acceptance Criteria, Dependencies**. The Architect determines internal Java design.
- **All 21 Markdown task files exist** under `docs/tasks/`. **All 21 files and contracts are APPROVED for implementation (2026-10-24)**. Execute each task only after its predecessor dependencies have been completed and independently verified; this index does not itself start Codex or services.

## Backlog: 21 slices total (approved split; milestone gates preserved)

### M1 — Payment Creation & Idempotency (2 approved)

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M1-01 — Transactional Payment Acceptance](../tasks/M1-01-transactional-payment-acceptance.md) | `POST /payments` validates input and commits Payment, AUTHORIZE/PENDING operation, scoped idempotency, audit and a pending Outbox record atomically; returns `202` after commit. | Approved [M1 contract](../contracts/m1-payment-creation-contract.md) | **APPROVED** |
| [M1-02 — Idempotent Replay & Concurrency](../tasks/M1-02-idempotent-replay-and-concurrent-requests.md) | Equivalent replay returns the original Payment; conflicting key usage returns `409`; concurrent first requests create one logical payment. | M1-01 | **APPROVED** |

### M2 — Outbox Publisher & Kafka Transport (2 approved; after M1)

**Approved M2 contract (2026-10-24):** [Outbox/Kafka/Avro implementation contract](../contracts/m2-outbox-kafka-contract.md), including the accepted Avro record, Kafka key-only `paymentId`, topic and leasing/recovery rules. Run M2-01 then M2-02 **after finishing and verifying M1**.

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M2-01 — First Outbox Command Published](../tasks/M2-01-publish-first-outbox-command.md) | A committed `ExecutePaymentOperation` Outbox intent becomes a decodable Avro command in local Kafka using Schema Registry, `RecordNameStrategy`, `FULL_TRANSITIVE`, and the canonical `paymentId` UTF-8 key. Broker/registry in local Docker; command carries accepted identities, no real PSP processing. | M1-02; [approved M2 contract](../contracts/m2-outbox-kafka-contract.md) | **APPROVED** (after M1) |
| [M2-02 — Reliable Outbox Recovery](../tasks/M2-02-recover-outbox-publication.md) | Kafka downtime and publisher crash leave pending work recoverable; retries can republish the **same logical messageId** at least once and mark delivery progress only after durable broker acknowledgment. Verify no payment disappears and duplicate publication remains possible/safe. | M2-01; [approved M2 contract](../contracts/m2-outbox-kafka-contract.md) | **APPROVED** (after M2-01) |

Relevant accepted sources: [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md), ADRs [0011](../adr/0011-execute-payment-operation-command-envelope.md)–[0019](../adr/0019-canonical-utf8-uuid-kafka-command-key.md). **M2 resolves topic naming, Avro record/namespace, schema-ID wire identity, Publisher claiming and recovery in its approved contract.**

### M3 — Payment Processor & Simulated Authorization (5 approved; subject to dependencies)

**M3 approved contract:** [PSP simulation, GET, active UNKNOWN recovery, bounded workers and offsets](../contracts/m3-payment-processor-contract.md). This proposal intentionally advances minimal active AUTHORIZE reconciliation into M3 so Kafka offsets do not rely on the later M6 for independent recovery. **Implementation APPROVED; dependency gate applies.**

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M3-01 — Consume & Resolve Authorization](../tasks/M3-01-process-simulated-authorization.md) | A Kafka command drives the **existing** AUTHORIZE operation through a simulated PSP and durably records confirmed approval or decline plus append-only audit, without a new PaymentOperation on redelivery. The PSP call does not block Kafka polling. | M2-02; approved fake PSP/command processing contract | **APPROVED** (after prerequisites) |
| [M3-02 — Read Payment Status](../tasks/M3-02-read-current-payment-status.md) | A client can retrieve confirmed Payment state and relevant operation status via a **approved** `GET /payments/{paymentId}`, including after asynchronous authorization. No inference of financial success from command delivery. | M3-01; [approved M3 GET contract](../contracts/m3-payment-processor-contract.md) | **APPROVED** (after prerequisites) |
| [M3-03 — Safe PSP Attempts & UNKNOWN](../tasks/M3-03-bounded-psp-attempts-and-unknown.md) | Safe provider-idempotent calls use **at most 3 total attempts**, with **5/10/20 s** per-request timeouts. Definitive failures and declines differ; ambiguous outcomes remain `UNKNOWN` and do not reset the PSP budget after a Kafka replay. | M3-01; fake PSP idempotency/status contract | **APPROVED** (after prerequisites) |
| [M3-04 — Bounded Parallel Processing](../tasks/M3-04-bounded-payment-workers.md) | Main-topic baseline **4 partitions / 1 Kafka consumer / 4 async workers**, global bounded intake and a bounded per-payment FIFO wait queue; at most one locally active financial operation per `paymentId`, plus DB safety across ownership changes. | M3-01, M3-03; agreed queue limits/backpressure | **APPROVED** (after prerequisites) |
| [M3-05 — Durable Manual Offset Progress](../tasks/M3-05-durable-kafka-offset-progress.md) | Commit only the **highest contiguous** offsets eligible after committed financial result or independently durable recovery handoff. Crash/rebalance/late-worker tests show no skipped operation; in-memory queue admission is never proof of durability. | M3-04; agreed tracking/rebalance and UNKNOWN recovery semantics | **APPROVED** (after prerequisites) |

Sources: [ADR-0005](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0020](../adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md), [ADR-0021](../adr/0021-small-mvp-kafka-partition-and-worker-topology.md), [ADR-0022](../adr/0022-bounded-per-payment-waiting-queue.md), [BR-002](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-006](../requirement/BR-006-distinguish-technical-failure-from-decline.md), [BR-007](../requirement/BR-007-bounded-psp-retries.md).

**Sequencing caution:** M5's durable technical-retry handoff and M6's full reconciliation workflow are later. M3 **must not** prematurely commit an offset merely because PSP status is `UNKNOWN` or work was queued. The [approved M3 contract](../contracts/m3-payment-processor-contract.md) requires an **active independently recoverable UNKNOWN scanner** plus durable job before Kafka offset acknowledgment.

### M4 — Capture, Void & Domain Invariants (4 approved; subject to dependencies)

**M4 approved contract:** [capture/void API, PSP-authoritative authorization validity, original-operation reconciliation](../contracts/m4-capture-void-expiration-contract.md). Approved task division unchanged: M4-01 → M4-04 → M4-02 → M4-03; active independent recovery is required for CAPTURE/VOID UNKNOWN before offsets advance. **Implementation APPROVED; dependency gate applies.**

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M4-01 — Idempotent Full Capture Admission](../tasks/M4-01-admit-idempotent-full-capture.md) | Proposed capture HTTP endpoint atomically admits one `CAPTURE:PENDING` and an Outbox intent for an eligible AUTHORIZED Payment; equivalent-key replay is stable; changed key/body, concurrent capture and unresolved/conflicting operations do not create another capture. | M3-05; capture API/idempotency contract | **APPROVED** (after prerequisites) |
| [M4-04 — Expired Authorization Guard](../tasks/M4-04-guard-expired-authorizations.md) | Guard capture admission against **authoritatively** expired authorizations; transition to `EXPIRED` with audit and produce no CAPTURE/Outbox. An unconfirmed timestamp/timeout is not proof of expiry. | M4-01; expiry-evidence contract | **APPROVED** (after prerequisites) |
| [M4-02 — Execute Capture](../tasks/M4-02-execute-full-capture.md) | Generic worker reuses the expiry guard before calling PSP for an eligible CAPTURE and records confirmed `Payment:CAPTURED` / `CAPTURE:SUCCEEDED`. Duplicate delivery cannot capture twice. | M4-01, M4-04; fake PSP capture semantics | **APPROVED** (after prerequisites) |
| [M4-03 — Void Authorized Payment](../tasks/M4-03-void-authorized-payment.md) | Proposed void/cancel endpoint and generic worker execute an eligible VOID once, preserving original operation identity and yielding `CANCELED` **only on confirmed PSP void**. Block conflicting/unknown operations. | M4-02; void API/provider contract | **APPROVED** (after prerequisites) |

Sources: [BR-003](../requirement/BR-003-expired-authorizations.md), [BR-004](../requirement/BR-004-idempotent-capture-requests.md), [BR-008](../requirement/BR-008-block-conflicting-financial-operations.md), [state machines](../domain/payment-state-machines.md). **Capture/void endpoint shapes, PSP evidence, expiry behavior and error envelopes are approved in the [M4 contract](../contracts/m4-capture-void-expiration-contract.md).** **Review correction:** M4-04's authoritative expiry admission guard precedes M4-02; M4-02 must reuse that guard before a PSP capture call. Exact expiry evidence and safe uncertain-validity behavior follow the approved M4 contract.

### M5 — Technical Retry, DLQ & Durable Scheduling (4 approved; subject to dependencies)

**M5 approved contract:** [retry/DLQ envelopes, retention protection, Scheduler/Dispatcher and handoff](../contracts/m5-technical-retry-dlq-scheduler-contract.md). **Implementation APPROVED; dependency gate applies.**

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M5-01 — Durable Technical Retry Handoff](../tasks/M5-01-durable-technical-retry-handoff.md) | Transient Kafka-processing/infrastructure failure transfers recoverable context to the **single retry topic** before source offset acknowledgment, without starting a new PSP financial operation. | M3-05; retry envelope/topic contract | **APPROVED** (after prerequisites) |
| [M5-02 — Poison Message Quarantine](../tasks/M5-02-quarantine-poison-commands.md) | Permanently unprocessable commands go to one DLQ with diagnostic identity and recovery context; the original offset advances only after **durably confirmed** quarantine. No false payment decline. | M5-01; poison-message taxonomy and DLQ contract | **APPROVED** (after prerequisites) |
| [M5-03 — Persist Retry-Topic Schedules](../tasks/M5-03-persist-technical-retry-schedules.md) | A retry-topic consumer creates exactly one durable schedule per logical retry identity; duplicates are idempotent and offset is committed only after PostgreSQL persistence. | M5-01; schedule schema/dedup contract | **APPROVED** (after prerequisites) |
| [M5-04 — Dispatch Due Retries Safely](../tasks/M5-04-dispatch-due-technical-retries.md) | The dispatcher durably republishes eligible work with **10 s × 2** exponential delays, **±20% jitter**, hard **5-minute cap**, crash/restart and concurrent claim safety; preserves `operationId` and correlation, and never resets the PSP attempt budget. | M5-03; retry publication/claim/recovery contract | **APPROVED** (after prerequisites) |

Sources: [ADR-0006](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0007](../adr/0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008](../adr/0008-persistent-kafka-retry-scheduler.md), [ADR-0009](../adr/0009-exponential-backoff-with-jitter-for-kafka-retries.md). **No fixed retry-count-based DLQ for ongoing transient failures**; financial `UNKNOWN` takes the reconciliation path instead.

### M6 — PSP Reconciliation & Observability (4 approved; subject to dependencies)

**M6 approved contract:** [generalized reconciliation, 24h escalation, retention and E2E fault matrix](../contracts/m6-reconciliation-observability-acceptance-contract.md). **Implementation APPROVED; dependency gate applies.**

| Task | Observable outcome | Depends on | State |
| --- | --- | --- | --- |
| [M6-01 — Resolve UNKNOWN by Original PSP Identity](../tasks/M6-01-resolve-original-unknown-psp-operations.md) | Safe PSP status lookup resolves the **original** UNKNOWN operation with authoritative evidence, changes Payment only for confirmed results and appends audit; repeated or late results cannot duplicate financial effects. | M3-03, M4-02, M5-04; PSP lookup/evidence contract | **APPROVED** (after prerequisites) |
| [M6-02 — Escalate UNKNOWN After 24 Hours](../tasks/M6-02-escalate-unknown-after-24h.md) | Persisted `UNKNOWN` work older than **24 hours** becomes discoverable/escalated for reconciliation, remains unresolved without evidence and does **not** become FAILED or CANCELED by age. Repeated escalation is idempotent. | M6-01; escalation/visibility contract | **APPROVED** (after prerequisites) |
| [M6-03 — Traceability & Retention](../tasks/M6-03-traceability-and-retention.md) | Correlated, sanitized HTTP/worker/PSP/Kafka diagnostics and meaningful metrics support following payment to audit and retry outcomes, while retaining **financial history/audit 5 years**, **technical logs 30 days** and **Kafka messages 7 days** under documented local policies. | M3–M5 behavior; logging/metrics/retention design | **APPROVED** (after prerequisites) |
| [M6-04 — End-to-End Failure Recovery](../tasks/M6-04-end-to-end-failure-recovery.md) | Automated integration scenarios prove recovery across lost HTTP replies, outbox duplicate publish, Kafka redelivery, consumer crash/rebalance, scheduler restart and ambiguous PSP results, without multiplying logical operations or claiming false success. | M6-01 through M6-03; agreed fault-injection/test acceptance | **APPROVED** (after prerequisites) |

Sources: [BR-002](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-005](../requirement/BR-005-reconciliation-after-24h-unknown.md), [BR-006](../requirement/BR-006-distinguish-technical-failure-from-decline.md), [NFR-001](../requirement/NFR-001-transaction-traceability-and-retention.md), [ADR-0002](../adr/0002-append-only-audit-and-retention.md). No live financial credentials or real card data.

## Approved dependency chain and execution gates

**Default implementation sequence:** `M1-01 → M1-02 → M2-01 → M2-02 → M3-01 → M3-02 → M3-03 → M3-04 → M3-05 → M4-01 → M4-04 → M4-02 → M4-03 → M5-01 → M5-02 → M5-03 → M5-04 → M6-01 → M6-02 → M6-03 → M6-04`. Task dependencies above, not this total order, are authoritative after review.

**All six contracts are approved** (2026-10-24): [M1](../contracts/m1-payment-creation-contract.md), [M2](../contracts/m2-outbox-kafka-contract.md), [M3](../contracts/m3-payment-processor-contract.md), [M4](../contracts/m4-capture-void-expiration-contract.md), [M5](../contracts/m5-technical-retry-dlq-scheduler-contract.md), [M6](../contracts/m6-reconciliation-observability-acceptance-contract.md). Start only with **M1-01**, then follow the prerequisite graph and independent tests/review from the local orchestration skill. No tasks have been implemented solely by this planning update.

**Review gate:** The user approved this **21-task division** on 2026-10-24. All individual six-section Markdown files now exist. **All M1–M6 tasks** are Codex-approved in dependency order, and all previously identified contract choices have an accepted specification. Agents must not invent conflicting business rules.
