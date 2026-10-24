# PaymentFlow — Review of the 21-task Vertical-Slice Backlog

- **Review date:** 2026-10-24
- **Final approval update (2026-10-24):** The user has explicitly **approved all four downstream M3–M6 contracts** and **all 17 remaining tasks**, bringing the total to **21 APPROVED implementation tasks across M1–M6**. The older review tables below document historical findings **as of earlier review gates** and are superseded where they describe M2–M6 as PLANNED or their contracts as open. See the [accepted M3–M6 decision review](m3-m6-contract-decision-review.md), [approved full backlog](proposed-complete-task-backlog.md), and [roadmap](../roadmap.md). **Execution still starts with M1-01, following task dependencies and independent tests; no application code was implemented or run by these document updates.**
- **Status update (2026-10-24, after this review):** The user explicitly **approved [M2 transport/Publisher contract](../contracts/m2-outbox-kafka-contract.md), [M2-01](../tasks/M2-01-publish-first-outbox-command.md) and [M2-02](../tasks/M2-02-recover-outbox-publication.md)** for implementation **after verified completion of M1**. **M1 + M2 = 4 approved tasks; M3–M6 = 17 PLANNED tasks**. The audit statements below about M2 being blocked describe the earlier pre-approval snapshot; they have been superseded by the linked approved contract. No implementation code, broker startup or Codex run has been performed in this approval update.
- **Scope:** Static review of the [full task backlog](proposed-complete-task-backlog.md), all 21 Markdown files in [`docs/tasks/`](../tasks/), [roadmap](../roadmap.md), relevant accepted requirements, domain model and ADRs.
- **Result:** **PASS for backlog documentation and approved task division; NOT an implementation or integration-test sign-off.** The two M1 tasks are independently approved for Codex implementation. The 19 tasks in M2–M6 are planned **but blocked on their milestone contracts/approval**.

## Second full review of all 21 task files — 2026-10-24

**Verdict:** **21/21 structurally sound and linked; NOT yet fully implementation-ready as a whole.** The two M1 tasks and two M2 tasks remain approved for sequential Codex work. The 17 tasks in M3–M6 remain planned and need specific contracts before execution. This is a GitHub-document/ADR review, **not** a compiled or integration-tested system.

**Checks repeated after corrections:** all 21 task files exist on `main`; each follows the same six approved headings in order; all relative task/requirement/ADR/contract links target existing repository paths; all 30 declared task-to-task dependency edges refer to existing tasks; **no dependency cycle** was found. No task status was accidentally escalated.

### Corrections implemented in this review

1. **M4 dependency/ordering safety:** updated **M4-04 to depend on M4-01** and to provide an independently testable **expiry check at capture admission**. **M4-02 now depends on both M4-01 and M4-04**, reuses that check before calling the PSP, and M4-03 remains after M4-02. Correct M4 task order: **M4-01 → M4-04 → M4-02 → M4-03**. The [backlog index](proposed-complete-task-backlog.md) was synchronized; task count is unchanged. Exact source of authoritative expiry evidence remains an **M4 contract gate**.
2. **M2-01:** removed possible exactly-once implication: M2 acceptance is **at least one** decodable Kafka record for a persisted publication.
3. **M2-02 and M2 contract:** tests now include **Schema Registry unavailability** as well as broker unavailability; **a stale Outbox claim holder cannot overwrite a newer lease/claim or mark incorrect delivery progress**. This is a clarification of already approved safe at-least-once lease behavior, not a new delivery model.
4. **M3-01:** explicitly requires disabling **consumer auto-commit from the first active consumer slice**; M3-05 retains responsibility for correct contiguous manual offset commits.
5. **M5-01:** explicitly rejects claiming complete/retention-independent retry recovery until **M5-03's persistent Retry Scheduler** exists. A Kafka retry topic by itself is only a time-limited buffer.

### Material concerns **still open** — do not silently design them in Codex

| Severity | Area | Review finding | Required before sign-off |
| --- | --- | --- | --- |
| **CRITICAL** | **M3-03 / M3-05 / M6-01** | `UNKNOWN` must not be acknowledged on Kafka until an **independently durable and actually recoverable reconciliation responsibility** exists (ADR-0005). The M3 drafts currently permit retaining the offset uncommitted, which is financially safe but can cause partition head-of-line blocking **through M4/M5 while full reconciliation is deferred to M6**. | During the **M3 contract**, decide whether to introduce a minimal **active** durable UNKNOWN recovery handoff/scanner before M3-05 commits, or expressly accept a test-only uncommitted/blocked offset and prohibit declaring the interim consumer operationally complete. No assumed recovery from a passive row alone. |
| **HIGH** | **M4-01 / M4-04 / M4-02** | The original order let capture execution precede authoritative expiry validation; **ordering is now corrected**, but provider evidence of *still valid vs definitively expired vs unknown* is unspecified. | Approve trusted PSP expiration evidence, admission/worker eligibility, safe deferral for ambiguous validity and the HTTP conflict/error contract. |
| **HIGH** | **M5-01 / M5-03 / M5-04** | The technical retry topic's 7-day retention can expire retry messages before durable scheduling if the ingestion path is unavailable too long; Kafka acknowledgment alone is not indefinite recovery. | Approve monitoring, retention/backlog safety and operational activation requirements; do not treat M5-01 alone as complete end-to-end technical retry recovery. |
| **MEDIUM** | **M3-02 / M3-04** | `GET /payments/{paymentId}` remains a proposed endpoint; global/per-payment FIFO bounds, backpressure/rebalance control and operation response schema are not yet approved. | Close the M3 API and worker-count/queue contract before invoking those tasks. |
| **MEDIUM** | **M6-03 / M6-04** | Retention/observability and end-to-end fault-injection slices have unusually broad acceptance for single slices; tracing storage for 30 days and 5-year history enforcement have no executable local policy yet. | Narrow acceptance to demonstrable local policy, deterministic tests and documented exclusions in the eventual M6 contract; revisit slice size only if implementation shows they are too large. |

### Ready-for-Codex gate after this review

- **M1-01 → M1-02:** contract approved, ready for deliberate local Codex execution.
- **M2-01 → M2-02:** contract approved, **only after M1 is complete and independently verified**.
- **M3-01 through M6-04:** **PLANNED**, not yet executable. Their six-section task files are reviewable; the listed contract decisions are still open.
- No Java implementation, test execution, Kafka startup or financial simulator run took place during this review.

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
