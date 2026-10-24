# PaymentFlow — Review of the 21-task Vertical-Slice Backlog

- **Review date:** 2026-10-24
- **Scope:** Static review of the [full task backlog](proposed-complete-task-backlog.md), all 21 Markdown files in [`docs/tasks/`](../tasks/), [roadmap](../roadmap.md), relevant accepted requirements, domain model and ADRs.
- **Result:** **PASS for backlog documentation and approved task division; NOT an implementation or integration-test sign-off.** The two M1 tasks are independently approved for Codex implementation. The 19 tasks in M2–M6 are planned **but blocked on their milestone contracts/approval**.

## Structural and dependency checks

| Check | Review result |
| --- | --- |
| Task inventory | **21** unique task IDs: M1 = 2, M2 = 2, M3 = 5, M4 = 4, M5 = 4, M6 = 4. |
| Task files | All 21 `docs/tasks/Mx-nn-*.md` files exist on `main`. |
| Format | All files contain exactly the agreed sections in order: Objective, Required Behavior, Scope Lock, Authoritative Sources, Acceptance Criteria, Dependencies. |
| File links | Internal links to other tasks, ADRs, requirements and domain documents resolve to paths in the repository tree; **no broken Markdown file links found**. |
| Dependency structure | Every declared task dependency refers to an existing task; no dependency cycle identified. Sequencing is a *default*, not authorization to implement before gates. |
| Authorization boundaries | M1-01 and M1-02 are APPROVED for implementation. M2-01 through M6-04 explicitly remain **PLANNED, not Codex-executable** until their milestone scope/contracts are approved. |
| Runtime verification | **Not performed.** No Java build, migration, integration test, Kafka startup or PSP execution was part of this documentation review. |

## Substantive ADR and business-rule alignment

**Aligned:** The backlog preserves M1's single-transaction admission + pending Outbox without Kafka or PSP; M2's at-least-once Kafka delivery and stable logical `messageId`; Avro/Schema Registry `FULL_TRANSITIVE` / `RecordNameStrategy`; the canonical `paymentId` Kafka key; M3's financial idempotency and **4 partitions / 1 consumer / 4 bounded workers**, FIFO per payment and manual contiguous offsets; M4's single full capture, explicit void and authoritative expiration; M5's distinct technical retry/DLQ/scheduler and approved 10-second base / exponential / jitter / 5-minute-cap backoff; M6's reconciliation, 24-hour escalation and separate 5-year audit, 30-day technical, 7-day Kafka retention.

**Corrections applied during review:**

1. **M6-01:** Added explicit dependency on [M4-03 — Void Authorized Payment](../tasks/M4-03-void-authorized-payment.md). UNKNOWN reconciliation must account for AUTHORIZE, CAPTURE **and VOID** rather than only authorization/capture.
2. **M6-04:** Extended failure-recovery acceptance to include capture, void and authoritative expiration; explicitly references [M4-04](../tasks/M4-04-guard-expired-authorizations.md).
3. **M4-02:** Clarified the capture safety gate: unresolved authorization expiry must not be bypassed, and M4 is not complete before the [M4-04 confirmed-expiry guard](../tasks/M4-04-guard-expired-authorizations.md).
4. **Planning index/roadmap:** Linked each of the 21 task files and reconciled the **approved backlog division** with the **not-yet-approved M2–M6 implementation contracts**.

## Cross-milestone issues to close **before implementation**

These are **known design gates**, not defects invented as work for Codex to decide:

| Priority / stage | Decision gate | Why it matters |
| --- | --- | --- |
| **Critical — before M2** | Approve the local Kafka/Schema Registry topology, names/retention, final Avro record namespace/type and whether `paymentId` is also in the value; publisher claiming/acknowledgment and schema-identity strategy. | ADRs accept principles but do not define the executable producer/wire contract. |
| **Critical — before M3** | Establish fake PSP idempotency and authoritative status lookup, attempt classification, **UNKNOWN independent recovery strategy**, durable claim/ownership and consumer offset/rebalance semantics. | [ADR-0005](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md) prohibits confirming an UNKNOWN offset without independent durable recovery. **M3-05 explicitly withholds that offset until a safe mechanism exists.** If that recovery only becomes functional in M6, a partition can be blocked by UNKNOWN in the interim. This is safer than premature acknowledgment but requires deliberate staging. |
| **High — before M3** | Approve global/per-payment queue limits, backpressure behavior and whether to include the proposed `GET /payments/{paymentId}` endpoint plus its response contract. | [ADR-0022](../adr/0022-bounded-per-payment-waiting-queue.md) approves bounded FIFO but not numeric limits; GET is not an accepted endpoint yet. |
| **High — before M4** | Approve capture/void API and PSP semantics, idempotency scopes and source of **authoritative** expiration evidence; stage M4-02 happy-path capture safely until M4-04 is completed. | Expiration must not be guessed from a timestamp or timeout, and no additional conflicting external action may begin. |
| **High — before M5** | Define retry/DLQ envelopes and records, durable producer handoff, scheduler storage/dedup and dispatcher claim/release/republication ownership. | Technical retries must not be confused with financial attempts or advance offsets prematurely. Until M5 DLQ is available, a poison message must **not** be silently skipped. |
| **High — before M6** | Define PSP query/evidence mapping, 24-hour escalation visibility, local retention/observability enforcement and deterministic failure-injection acceptance. | UNKNOWN is not failed/canceled by age; audit must remain independently recoverable after Kafka/log retention windows. |

## Ready-to-execute vs. planning-only

- **Ready for deliberate local Codex invocation:** [M1-01](../tasks/M1-01-transactional-payment-acceptance.md), followed by [M1-02](../tasks/M1-02-idempotent-replay-and-concurrent-requests.md), under the [accepted M1 contract](../contracts/m1-payment-creation-contract.md). Neither task's implementation/testing has been performed by this review.
- **Planning-only:** All M2–M6 tasks. Their **task count/split was approved**; their milestone scope, exact technical contracts and implementation authorization are still pending.
- **Next planning action:** settle **M2's transport/publisher contract** while keeping M1 implementation independent. Update task details only with approved decisions.

This review verifies document structure, references and architectural consistency, **not** running-system correctness or financial safety in production.
