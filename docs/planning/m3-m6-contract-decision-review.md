# PaymentFlow — Full M3–M6 Contract-Decision Review

- **Date:** 2026-10-24
- **Status:** **DESIGN PASS APPROVED (2026-10-24)**. The user explicitly approved all four M3–M6 contracts and all 17 associated tasks for **Codex implementation after their predecessors are verified**. M1 and M2 approvals remain valid.
- **Method:** Review the missing business/API/data/operational choices against all [21 tasks](proposed-complete-task-backlog.md), [ADRs](../adr/), [BRs/NFR](../requirement/) and the [earlier task audit](task-backlog-review.md). The four downstream contracts are **accepted implementation decisions**; this page records completeness and safety boundaries. This is a documentation review, not an executable system validation.

## Decision inventory by milestone

| Stage / tasks | Contract chosen for review | Previously missing decisions now approved |
| --- | --- | --- |
| **M3-01…05** | [M3 contract](../contracts/m3-payment-processor-contract.md) | Stateful PSP HTTP boundary and durable idempotency, source-of-truth mapping and evidence, GET Payment API/error shape, original financial attempt counting with 5/10/20s timeouts and 1/2s retry gaps, independent active UNKNOWN reconciliation, PROCESSING owner fencing, 4 partitions/1 consumer/4 workers, global=32/per-payment-wait=4/per-partition=8, poll max=16, manual contiguous offsets, crash/rebalance and poison blocking pending M5. |
| **M4-01,04,02,03** | [M4 contract](../contracts/m4-capture-void-expiration-contract.md) | Exact full capture and void HTTP endpoints/header/body/202/409 contracts, per-action idempotency and stable PSP operation identity, authoritative active-vs-expired vs unknown PSP evidence, fail-closed capture admission, admission+worker expiry checks, capture/void transitions and minimal active CAPTURE/VOID UNKNOWN recovery before offset acknowledgment. Safe order M4-01 → M4-04 → M4-02 → M4-03. |
| **M5-01…04** | [M5 contract](../contracts/m5-technical-retry-dlq-scheduler-contract.md) | Retry/DLQ topic names/topology/retention, separate Avro record subjects/fields, deterministic UUIDv5 retry/quarantine source identities, raw writer-schema wire bytes, poison taxonomy, durable before-offset handoff, 7-day-retention-safe SQL quarantine, schedule schema and dedup/lineage, 10s×2 jitter/capped delay, dispatcher schedule+Outbox atomic handoff/claim/fencing, lag alerts and recovery limits. |
| **M6-01…04** | [M6 contract](../contracts/m6-reconciliation-observability-acceptance-contract.md) | Generalized authoritative operation evidence and conflict resolution without redoing minimal M3/M4 recovery, 24-hour first-UNKNOWN durable escalation, scanner cadence/lease, audit/structured logs/Micrometer, 5-year financial/30-day technical/7-day Kafka retention enforcement with holds, safe local cleanup, explicit 10-case deterministic E2E fault matrix. |

## Critical invariants preserved across milestones

1. **One logical PaymentOperation per financial action; PSP original idempotency identity is stable.** Kafka `messageId` is publication identity, not payment uniqueness. Producer/pub retries can duplicate records, without duplicating external financial effects.
2. **UNKNOWN recovery is active before Kafka offset progress:** M3-03 installs durable jobs + independent AUTHORIZE status scanner. M3-05 acknowledges an UNKNOWN only after job/active scanner exist. M4-02 and M4-03 extend active lookup for CAPTURE/VOID UNKNOWN **before** their source offsets may be acknowledged. M6-01 then adds generalized evidence handling, not a second independent recovery service.
3. **Never confuse authorization expiry, explicit VOID or ambiguous CAPTURE.** M4-01 fails closed without positive PSP authorization-validity evidence, M4-04 records definitive EXPIRED, and M4-02 reuses guard immediately before PSP call. A timeout is neither proof of expiry nor proof of CAPTURE success.
4. **Separate technical Kafka backoff and PSP HTTP budget.** Technical retries use the accepted 10s/2×/±20%/300s policy and durable scheduler; PSP financial attempts remain max 3 total, 5/10/20s per-request, preserved across redelivery.
5. **Offsets only after committed state or durable independent handoff**, tracking contiguous eligible offsets by partition/generation. During partial implementation (before M5 DLQ and retry scheduler), poison/transient messages can block; they are never silently skipped.
6. **Financial audit remains authoritative independently of Kafka and log retention.** SQL recovery jobs/quarantine evidence survive 7-day Kafka deletion; no deletion for open UNKNOWN/conflicting work or before the financial minimum retention.

## Review observations / non-architectural execution choices

- **Approval recorded:** on 2026-10-24 the user accepted the four contracts and authorized the remaining 17 tasks in dependency order. This approval does not auto-run agents, merge pull requests, or attest to passing implementation tests.
- **Architect freedom remains:** Java packages/classes, concrete repository SQL style, Maven module organization, worker library interfaces, dependency versions compatible with the approved runtimes, and specific code-level exception types are implementation-level design, **not missing financial/business contracts**. If implementation reveals a new material behavioral conflict, route it back for decision rather than inventing business semantics.
- **Local-only trade-offs:** single broker replication factor 1, simulated PSP in isolated runtime/storage, localhost operational endpoints, no high-availability guarantee, no live financial credentials, no performance SLA, no PCI certification.
- **Not guaranteed by planning alone:** no static documentation audit can prove real Kafka rebalance, PSP failure or 5-year retention behavior. Automated tests and independent Test Engineer evidence are still needed task by task.

## Gate recommendation

**Approved as one package on 2026-10-24.** All six contracts and 21 six-section task files now carry accepted implementation status **subject to upstream task dependencies, independent tests and specialist gates**. The safe order starts at [M1-01](../tasks/M1-01-transactional-payment-acceptance.md); no Codex run or implementation code change occurred as part of the approval documentation.
