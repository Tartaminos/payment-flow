# ADR-0020 — Bounded Asynchronous Payment Processing with Per-Payment Exclusion

- **Status:** Accepted
- **Date:** 2026-10-23
- **Decision scope:** Payment Processor consumption-to-worker concurrency strategy for the learning-oriented MVP
- **Related:** [ADR-0004 — Outbox and asynchronous PSP execution](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Durable-before-commit Kafka offsets](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0006 — Technical retries versus PSP reconciliation](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0008 — Persistent Retry Scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0018 — paymentId as partition key](0018-payment-id-as-kafka-command-partition-key.md), [ADR-0019 — Canonical UTF-8 paymentId key](0019-canonical-utf8-uuid-kafka-command-key.md), [BR-008 — Block conflicting financial operations](../requirement/BR-008-block-conflicting-financial-operations.md)

## Context

The PaymentFlow project is a **documentation-first educational architecture**, not a production payments platform with a large, measured traffic requirement. The goal is to exercise safe asynchronous processing, financial idempotency and recoverability without overengineering capacity.

Sequentially performing a blocking PSP HTTP call on a Kafka consumer/poll thread would make that thread unavailable while the simulated PSP is slow. A second candidate was to have Kafka consumers hand off command processing to a **small, bounded asynchronous worker pool**, while managing concurrency for each `Payment`.

At this ADR's acceptance, numerical capacity remained open. **[ADR-0021](0021-small-mvp-kafka-partition-and-worker-topology.md) subsequently approves a small local baseline of 4 main-topic partitions, 1 Payment Processor Kafka consumer instance and 4 asynchronous worker slots.** No actual throughput or p95/p99 latency target is accepted. Prior examples of 20 or 40 commands/second were **teaching assumptions only**, not system requirements.

## Decision

**Decouple Kafka polling from simulated PSP command processing by using a bounded asynchronous worker pool, with at most one actively executing financial operation per `paymentId` at a time. Favor intentionally modest local-MVP capacity over production-scale sizing.**

1. **Consumer responsibility:** Kafka consumers poll and decode `ExecutePaymentOperation` Avro commands, recover the stable `paymentId` Kafka key and submit *eligible* work to a **bounded** processing mechanism. Actual simulated-PSP execution is performed outside the Kafka polling thread.
2. **Bounded work:** The number of simultaneously executing jobs and queued uncompleted jobs must be **limited**. When limits are reached, apply backpressure instead of accepting unbounded in-memory work or generating new financial operations. Exact bounds and intake/pause mechanics remain to be chosen.
3. **Per-payment exclusion:** Within one active worker ownership scope, never execute two financial commands concurrently for the same `paymentId`. If commands for the same payment are already received from one Kafka partition, preserve their observed order of admission to that local per-payment work sequence (unless safely classified and durably deferred under existing decisions).
4. **Cross-instance and cross-topic protection:** An in-memory `paymentId` queue/lock is **only a local coordination optimization**, not the definitive financial safeguard. Kafka consumer rebalances, the main/retry topic split, delayed dispatch and multiple processes can create concurrent work for the same payment. PostgreSQL transactional/conditional state checks and PSP idempotency/reconciliation remain authoritative per BR-002/004/008 and earlier ADRs. The **specific database claim/lease/transition mechanism is not approved yet**.
5. **Manual offset commits:** Submitting work to an in-memory queue, starting a background task or acknowledging an executor's acceptance **does not qualify as a durable processing result**. Under ADR-0005, a Kafka record becomes committable **only after** durable business state, a recoverable retry handoff, or quarantine. If different records from one partition finish out of order, acknowledge **only the highest contiguous eligible offset range**, never skip an earlier incomplete offset.
6. **Consumer lifecycle:** A partition reassignment/rebalance, process crash or stalled worker must not cause an in-flight financial operation to be silently considered complete. Work with no recorded durable result may be redelivered and must safely recover using the same `PaymentOperation` and stable PSP identity. Worker cancellation, leases, rebalance coordination and polling liveness are future detailed-design topics.
7. **Failure policy:** Transient Kafka-side failures follow the one-retry-topic and persistent-scheduler decisions (ADRs 0006–0009); uncertain PSP outcomes follow `UNKNOWN` and reconciliation, **not** an automatic new charge or unbounded PSP retry.
8. **MVP scale:** Choose **few partitions, a small consumer group and a small worker pool** for local learning. **[ADR-0021](0021-small-mvp-kafka-partition-and-worker-topology.md) subsequently specifies 4 main-topic partitions, 1 Kafka consumer instance and 4 workers**. No claim of high-scale performance, no arbitrary fixed throughput SLA and no need for elaborate distributed worker infrastructure in the MVP.

## Conceptual flow

```text
Kafka operation commands (key = paymentId)
                   |
                   v
       Consumer: poll/decode/validate
                   |
                   v
        Bounded pending-work intake
                   |
            Per-payment gating
                   |
          +--------+--------+
          |                 |
     Worker for A      Worker for B
          |                 |
     Payment A DB       Payment B DB
     checks/PSP          checks/PSP
          |                 |
          +--------+--------+
                   |
       Durable outcome / durable
           handoff / DLQ
                   |
       Contiguous Kafka offset commits

Same payment:  A1 -> A2, not A1 || A2 within one owner.
Cross-topic redelivery/retries: database state still decides safety.
```

These boxes are **logical responsibilities**, not new services or Java class definitions.

## Key failure/throughput cases

| Scenario | Required response |
| --- | --- |
| Simulated PSP is slow for Payment A | A worker may remain occupied, but independent Payments B/C can proceed within the bounded concurrency budget; do not execute a second conflicting operation for A. |
| Two commands for Payment A arrive before first completes | Serialize or durably defer the later work; do not make concurrent PSP calls for the same Payment merely because there are two workers. |
| Worker for later offset finishes before earlier offset | Retain the later durable completion locally, but **do not commit past the earlier outstanding Kafka offset**. |
| Consumer crashes after enqueue but before durable processing | A Kafka redelivery must remain possible; in-memory enqueue never counts as safe offset acknowledgment. |
| Worker/consumer ownership changes during a PSP call | Rely on durable PaymentOperation claims, idempotency identity and ambiguity/reconciliation policy; a process-local lock cannot prevent remote overlap. |
| Retry Dispatcher republishes an older command after a newer command is seen | Same `operationId` and `paymentId` identity; re-check persisted state, never infer cross-topic order from Kafka. |
| All worker/queue capacity is busy | Apply bounded backpressure, not infinite buffering, premature commits, nor a false financial success/failure state. |

## Alternatives considered

### Execute PSP calls sequentially on Kafka polling threads

**Not selected as the primary model.** It can simplify offset tracking and partition-local order but ties poll progress to external HTTP latency and makes it harder to demonstrate independent processing of different payments.

### Unbounded asynchronous worker pool / queue

**Rejected.** Makes failure recovery, memory pressure, resource usage and Kafka consumer progress harder to reason about without providing educational value.

### Small bounded asynchronous workers, per-payment exclusion and DB guards

**Accepted.** Demonstrates useful architectural trade-offs while remaining implementable in a modest educational MVP.

## Deliberate scope limits and open decisions

- **[ADR-0021](0021-small-mvp-kafka-partition-and-worker-topology.md) resolves the initial main-topic/processor numbers:** 4 partitions, 1 Kafka consumer instance and 4 concurrent asynchronous workers. **Pending queue bound, backpressure details, retry/DLQ topic topology and representative simulated test load remain TBD.**
- Worker scheduling fairness, maximum in-flight per partition, local keyed-queue implementation, backpressure and Kafka poll/rebalance strategy.
- Durable DB operation claims, timeout/lease fencing and PSP request lifecycle; a local mutex does not satisfy BR-008 by itself.
- Full technical retry record shape, scheduler persistence, dispatcher handoff and offset bookkeeping mechanics.
- Key migration/partition-expansion policy, benchmark measurement plan and measurable success criteria.
- Whether `paymentId` also appears in the Avro value; **not added by this ADR**.

## Documentation-only boundary

This ADR chooses a **concurrency model and pragmatic MVP scale**, not production sizing. It creates no Java code, Kafka topics/configuration, database migrations, schema files, services or runnable benchmarks.
