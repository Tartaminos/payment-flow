# ADR-0009 — Exponential Backoff with Jitter for Kafka Consumer Retries

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Delays between retries of transient Kafka consumer-processing failures
- **Related:** [ADR-0005 — Manual offset commits](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0006 — Processing failures versus financial reconciliation](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0007 — One retry topic and one DLQ](0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008 — Persistent retry scheduler](0008-persistent-kafka-retry-scheduler.md), [BR-007 — Bounded PSP HTTP attempts](../requirement/BR-007-bounded-psp-retries.md)

## Context

PaymentFlow already has an accepted logical architecture with one Kafka retry topic, one DLQ and a **persistent Retry Scheduler plus Dispatcher**. When a consumer encounters a transient infrastructure/processing error (for example, temporary PostgreSQL unavailability), immediate repeated attempts can amplify the original outage and create a thundering herd after recovery.

The timing policy must control the **Kafka message-processing retry** only. A Kafka retry is not equivalent to a new PSP financial HTTP attempt; the approved PSP request-timeout and safe-retry contract is governed separately by BR-007.

## Decision

Adopt **exponential backoff with bounded multiplicative jitter** for transient Kafka consumer-processing retries:

| Parameter | Accepted value |
| --- | --- |
| Base delay before the first Kafka processing retry | **10 seconds** |
| Exponential growth factor | **2** |
| Maximum scheduling delay | **300 seconds (5 minutes)** |
| Jitter | **Uniform random variation of ±20%** of the capped nominal delay |
| Handling of a sustained transient failure | **Continue recoverable, bounded-rate retries with operational alerting; do not classify as a poison message solely because of elapsed time or retry count** |
| Automatic DLQ after a fixed retry count | **No** |

The conceptual delay for retry number `n` (where the first retry is `n = 1`) is:

```text
nominalDelaySeconds = min(10 * 2^(n - 1), 300)
jitterFactor        = Uniform(0.8, 1.2)
delaySeconds        = min(300, nominalDelaySeconds * jitterFactor)
retryAt             = failureRecordedAt + delaySeconds
```

This defines a **hard five-minute maximum**; at the cap, jitter values exceeding 300 seconds are clipped rather than extending the maximum. It therefore does **not** guarantee a symmetric ±20% range after clipping. Exact precision/rounding, clock handling and random-number mechanics are implementation details.

### Illustrative schedule

| Kafka processing retry | Nominal delay | Possible delay after jitter and hard cap |
| --- | ---: | ---: |
| 1 | 10s | 8–12s |
| 2 | 20s | 16–24s |
| 3 | 40s | 32–48s |
| 4 | 80s | 64–96s |
| 5 | 160s | 128–192s |
| 6 and later | 300s | 240–300s |

The table shows **intervals between eligible attempts**, not individual HTTP request timeouts to the PSP.

## Application within the accepted retry architecture

```text
Kafka main operation-request topic
             |
             v
       Payment Processor
             |
             | Transient consumer-processing failure
             v
       Kafka retry topic
             |
             | Retry consumer durably persists schedule
             v
       Persistent Retry Scheduler
             |
             | retryAt calculated using backoff + jitter
             v
       Durable pending retry record
             |
             | Time-eligible
             v
       Retry Dispatcher
             |
             | Independently durable reprocessing handoff
             v
       Payment Processor
             |
             | Fresh DB state and idempotency check
             v
     Safe processing or new classification
```

- The retry-topic consumer commits its offset **only after** the schedule is persisted durably and idempotently (ADR-0005 and ADR-0008).
- The Dispatcher must durably hand off due work and tolerate crashes and duplicates; its concrete Outbox/event contract remains open.
- Retry metadata must preserve the **same logical operation and original event correlation**. Scheduling a new consumer attempt **never resets** `PaymentOperation.attempt_count` or creates another independent financial operation.
- **Time eligibility** (`retryAt <= now`) is not **financial eligibility**. Every execution must revalidate authoritative Payment/PaymentOperation state and existing concurrency/idempotency rules in PostgreSQL.
- The schedule does not require keeping a Kafka partition paused, nor does the message automatically wait inside Kafka until `retryAt`.

## Prolonged outages, alerts and DLQ boundaries

A database can be temporarily unavailable for a prolonged period. Such an event must **not** become a permanent-message error just because retries have been attempted many times.

1. Keep retries **recoverable** using the persistent schedule and bounded delay; avoid fast/unbounded retry loops.
2. Generate operational alerts and expose retry backlog/age when failures persist. **Specific alert thresholds, escalation ownership and circuit-breaking/load-shedding policy remain to be defined.**
3. A definitively invalid, unsupported or otherwise unprocessable Kafka message belongs in the **durable DLQ/quarantine** under ADR-0006, not in an endless transient retry loop.
4. A PSP timeout with an uncertain external financial result belongs in `PaymentOperation: UNKNOWN` with **independent reconciliation**, not the Kafka retry loop or DLQ. A consumer redelivery never authorizes a new financial charge.
5. Kafka's initially agreed **seven-day retention** is not a guarantee of limitless recovery while the schedule store is unavailable. Monitor retry-topic retention, capacity and lag; long-outage recovery remains an operational design task.

## Alternatives considered

### Immediate or fixed-delay Kafka consumer retries

**Rejected as the default.** They concentrate repeated requests and can worsen an infrastructure outage.

### Exponential backoff without jitter

**Not selected.** Although it reduces retry frequency, consumers failing together can still retry together.

### Exponential backoff with ±20% jitter and a hard five-minute cap

**Accepted.** Spreads retry attempts and bounds the wait without requiring multiple Kafka topics per delay tier.

### Sending events to DLQ after a fixed number of transient retries

**Rejected.** Retry exhaustion alone does not prove a message is malformed or permanently unprocessable and says nothing about the financial outcome.

## Consequences

- Retries become progressively less frequent during sustained failures, reducing coordinated load spikes.
- The actual retry time is not deterministic because of jitter and scheduling/dispatch lag.
- Persistent schedule identities, deduplication, durable dispatch and operation-level idempotency remain essential.
- The initial Kafka retry topology remains one retry topic plus one DLQ; the persistent scheduler is unchanged.
- A permanently unavailable dependency still requires intervention; this ADR specifies retry pacing, **not** a recovery-time SLA or infinite-retention guarantee.

## Still open

- Exact scheduler schema, claim/lease semantics, schedule time precision and dispatcher Outbox/handoff contract.
- How to derive the monotonic Kafka processing retry number across redelivery/republishing without accidentally incrementing on duplicate delivery.
- Operational alert thresholds, backlog limits, overload controls, monitoring, and aged-retry escalation.
- Topic names, topic message contracts, partition keys, consumer groups and deployment sizing.
- Separate **PSP HTTP retry delays**: this ADR does not set backoff between the already accepted 5s/10s/20s **per-attempt request timeouts**.
- Safe handling of cross-topic reordering and valid-but-not-yet-eligible financial operations.

## Documentation-only boundary

This ADR records a design decision only. No application code, SQL migrations, Kafka topics, configurations or deployed infrastructure are created.
