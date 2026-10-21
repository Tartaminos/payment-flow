# ADR-0008 — Persistent Scheduler for Kafka Consumer Retries

- **Status:** Accepted
- **Date:** 2026-10-21
- **Scope:** Durable delayed scheduling of Kafka consumer-processing retries
- **Related:** [ADR-0004 — Asynchronous payment processing and Transactional Outbox](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Manual Kafka offset commits](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0006 — Consumer retry versus financial reconciliation](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0007 — Single retry topic and DLQ](0007-single-kafka-retry-topic-and-dlq.md)

## Context

ADR-0007 approved **one Kafka retry topic** for transient consumer-processing failures and **one DLQ** for definitively unprocessable records. Kafka retains and delivers records, but the retry topic does not by itself provide a durable "do not process before this time" scheduling facility.

A retry consumer that simply waits or pauses a partition until a record becomes eligible can delay unrelated retry records (*head-of-line blocking*). The project therefore chooses a **persistent Retry Scheduler**, rather than relying on consumer-local timers and long pauses as the primary retry scheduling mechanism.

This mechanism is **separate** from PSP HTTP retry rules in [BR-007](../requirement/BR-007-bounded-psp-retries.md), which already permit at most three PSP financial HTTP attempts (5s/10s/20s per-request timeout budgets) only when external idempotency makes each attempt safe.

## Decision

**A dedicated logical Retry Scheduler will durably register retry work, its eligibility time and its recovery identity. A separate logical Dispatcher will release due work for processing.**

1. The Payment Processor classifies a **transient consumer-side/infrastructure failure** and transfers its message to the **single retry topic** with a durable Kafka acknowledgment before committing the original topic offset, in accordance with ADR-0005 and ADR-0007.
2. A **Retry Scheduler ingestion consumer** reads the retry-topic record, validates its context and **persists a scheduling responsibility** (including a stable deduplication identity, existing Payment/PaymentOperation references when available, attempt metadata and a `retryAt` or equivalent eligibility time) in a durable store.
3. **Only after the schedule is durably committed** may the retry-topic consumer commit the corresponding Kafka offset. On redelivery it must recognize already persisted schedules idempotently, rather than creating duplicate retry responsibilities.
4. A **Retry Dispatcher** periodically identifies eligible, outstanding schedules and triggers processing using a **durable handoff**. It must not mark a schedule as successfully handed off merely because it attempted to send a message.
5. The logical scheduler and dispatcher may be modules/jobs in the same deployment; this ADR does not require dedicated microservices.
6. A persistent store is required. **PostgreSQL is the natural candidate** alongside the current transactional model; a conceptual supporting `scheduled_retries` table is proposed, but its physical schema, retention, keys and transaction/lease mechanics are not yet approved.
7. **A retry topic remains part of the accepted design** (ADR-0007). Persistent scheduling adds a second durable boundary; it does not replace the retry topic or DLQ.

### Conceptual flow

```text
Main Kafka operations topic
           |
           v
    Payment Processor
           |
           | Transient consumer processing failure
           v
   Kafka retry topic (durably acknowledged)
           |
           | Source offset may then be committed
           v
  Retry Scheduler ingestion
           |
           | Persist identity + next eligible time
           v
    Durable retry schedule
           |
           | Due work selected by dispatcher
           v
     Retry Dispatcher
           |
           | Durable dispatch/handoff (mechanism pending)
           v
  Payment Processor workflow
           |
           | Recheck DB operation state and idempotency
           v
   Process only if financially safe

Definitively invalid message -> durable DLQ/quarantine
Uncertain PSP outcome        -> UNKNOWN + reconciliation
```

A possible dispatch strategy is to register a **new Outbox publication intent** in the same database transaction as the schedule state update, then publish a reprocessing command to Kafka. This fits ADR-0004 but is a **design candidate**, not an approved specific event contract, table schema or topic routing.

## Critical failure handling

| Failure or race | Required behavior |
| --- | --- |
| PostgreSQL unavailable when original Payment Processor fails | The original consumer can durably publish to the **Kafka retry topic**, then acknowledge the original offset; if retry publication cannot be acknowledged, do **not** advance the source offset. |
| PostgreSQL unavailable when Retry Scheduler reads retry topic | Cannot persist schedule yet; **do not commit that retry-topic offset**. Kafka retains the record according to its retention policy until processing can resume. Backpressure/operational recovery is required for prolonged outages. |
| Scheduler persists schedule but crashes before committing Kafka offset | Kafka redelivers; ingest idempotently based on the stable retry identity. |
| Dispatcher crashes before it durably hands off due work | The schedule remains pending/recoverable and becomes eligible for redispatch. |
| Dispatcher publishes work but crashes before marking it dispatched | Duplicate dispatch may occur; the durable handoff and Payment Processor must be idempotent. A transactional Outbox can mitigate lost-publication races. |
| Several dispatcher instances select the same due record | They require database-safe claiming/leases or equivalent conditional coordination. Exact mechanics remain open. |
| A retry becomes due after a relevant PaymentOperation already completed | Consumer re-reads authoritative state and performs no new PSP financial effect; it safely acknowledges the duplicate. |
| PSP call may have occurred before original worker crashed | **Never automatically issue another financial call** merely because scheduled work is due. Resolve/reconcile the **same logical PaymentOperation**, preserving its stable PSP identity and BR-002/BR-008 constraints. |
| Retry-topic backlog exceeds Kafka retention during a long database outage | Kafka storage is not an unlimited persistence guarantee; monitor backlog and retention, and define a capacity/recovery plan. |
| Retry handoff changes event order relative to other payment operations | Database-enforced business preconditions and durable handling of valid-but-not-yet-eligible work remain mandatory. |

## Safety invariants

- **Scheduling is not a financial result.** Recording, dispatching or completing a Kafka retry schedule does not change `Payment.status` to `AUTHORIZED`, `CAPTURED`, `DECLINED` or `FAILED` on its own.
- **Kafka retry attempt counts and PSP HTTP attempt counts are separate**. A reprocessed Kafka message must never reset the PSP three-attempt budget or invent a new independent financial operation.
- Original `eventId`, `operationId` and `paymentId` (where available) must remain traceable through retries; the deduplication identity of each scheduled work item and its retries needs a stable, deterministic contract.
- **Durable-before-ack:** every Kafka offset commit is preceded by the durable business outcome or independently recoverable handoff defined in ADR-0005.
- The scheduler is responsible for **time eligibility**, while Payment Processor/database rules are responsible for **financial eligibility**. These are independent checks.
- Delayed work can be re-ordered across Kafka topics. Never use partition order as a substitute for authoritative payment and operation-state validation.
- Explicit quarantine/DLQ remains reserved for **definitively unprocessable consumer messages** (ADR-0006), not expired timeouts, PSP uncertainty or an arbitrary retry-count threshold.
- Do not retain cardholder secrets/raw sensitive PSP payloads in retry schedules or Kafka messages.

## Alternatives considered

### Consumer-local pause/resume or sleeping until `retryAt`

**Not selected as the primary scheduling mechanism.** Simpler persistence story, but may block a partition behind a distant eligibility time, risks rebalancing complications, and ties scheduling progress to one live consumer.

### Multiple retry topics with different delay tiers

**Not selected for the MVP.** This conflicts with the previously approved single-retry-topic design and adds routing/topology overhead.

### One retry topic plus persistent scheduler and due-work dispatcher

**Accepted.** Allows retry messages to be durably stored and later made eligible without relying on a partition being paused until the retry deadline.

## Consequences and costs

- The initial topology gains a durable scheduling responsibility, recovery/deduplication requirements, dispatcher coordination and operational monitoring.
- With PostgreSQL as the schedule store, an outage of PostgreSQL also temporarily prevents retry ingestion and due-work dispatch. **The Kafka retry topic buffers unacknowledged retry messages until recovery**, subject to retention/backlog limits; the design must not falsely claim independent scheduler availability during database outages.
- Persisting a schedule and dispatching a due message must remain recoverable across separate crashes. This is another reliable-handoff problem, not something Kafka offsets alone solve.
- A scheduler avoids the specific *wait-until-eligible* bottleneck on a retry partition; it does not eliminate database outage, ordering, throughput or overall system-backpressure concerns.

## Not yet approved

- **Kafka retry backoff formula and parameters**: exponential backoff and jitter were discussed, with illustrative 10-second initial delay, 5-minute cap and ±20% jitter, but these are **proposals**, not committed values.
- Maximum attempts, age/expiry of scheduled retries, alert thresholds and what to do after sustained transient failures (do **not** equate exhaustion with permanent poison-message status).
- Exact physical schema, deduplication keys, indexes, claiming/leases, scheduling precision, clocks/timezones, scheduler poll frequency and dispatch batch size.
- Whether dispatch uses the existing Transactional Outbox, exact destination Kafka topic, event contract, message key, consumer groups, and delivery acknowledgement.
- Mechanism for preserving **valid but temporarily ineligible** financial work under reordering.
- Monitoring, operational replay and long-outage handling under Kafka's seven-day initial retention policy.
- Whether PSP reconciliation needs a distinct scheduler; this ADR **does not** equate financial reconciliation with Kafka consumer retries.

## Documentation-only boundary

This ADR records an accepted architecture choice. It creates no Java application code, SQL/migrations, Kafka configuration or deployed infrastructure.
