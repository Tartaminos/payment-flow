# ADR-0021 — Small MVP Kafka Topology: Four Partitions, One Consumer and Four Workers

- **Status:** Accepted
- **Date:** 2026-10-23
- **Decision scope:** Initial local educational-MVP sizing for the main Payment Processor Kafka consumer and its bounded asynchronous worker pool
- **Related:** [ADR-0004 — Outbox and asynchronous processing](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Durable-before-commit Kafka offsets](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0008 — Persistent retry scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0018 — paymentId as Kafka key](0018-payment-id-as-kafka-command-partition-key.md), [ADR-0019 — Canonical key encoding](0019-canonical-utf8-uuid-kafka-command-key.md), [ADR-0020 — Bounded asynchronous workers and per-payment exclusion](0020-bounded-async-payment-processing-with-per-payment-exclusion.md)

## Context

PaymentFlow is **not a commercial payment system** and does not have a real production traffic requirement. The project's purpose is to document and eventually test a realistic but manageable set of reliability concerns, including asynchronous processing, partition affinity, offset durability, duplicate-message recovery and conflicting financial operations.

ADR-0020 accepted processing work outside the Kafka polling thread in a **bounded asynchronous worker pool**, with per-payment exclusion and authoritative database state checks. It intentionally deferred the numeric size. After comparing different hypothetical partition/consumer/worker configurations, the MVP selects a minimal baseline rather than sizing for an imagined large payment volume.

Earlier illustrative figures of 20/40 commands per second are **not requirements, promises or acceptance criteria**.

## Decision

**Use the following starting topology for the local educational MVP's *main* `ExecutePaymentOperation` consumption path:**

| Component | Initial baseline | Meaning |
| --- | --- | --- |
| Main financial-command Kafka topic | **4 partitions** | Partition affinity by canonical `paymentId` key (ADRs 0018–0019) |
| Payment Processor Kafka consumers | **1 active consumer instance in its consumer group** | This member can be assigned all four main-topic partitions |
| Asynchronous Payment Processor worker pool | **4 workers maximum executing simultaneously** | Process different payments in parallel, outside the Kafka polling thread |
| Per-payment active work | **At most 1 simultaneously active financial operation per `paymentId`** | Locally serialize conflicting work; rely on DB guards across rebalances, retry paths or instances |
| Pending work | **Bounded, exact queue limit deferred** | Backpressure must apply rather than unbounded memory growth |
| Simulated PSP | **Yes** | No real cardholder data, real money or real PSP credentials |
| Deployment context | **Local / learning MVP** | Not a production throughput or resilience target |

One Kafka consumer instance does **not** mean one worker thread. The Kafka consumer handles poll/decode/intake, while four **separate bounded worker execution slots** perform asynchronous application work.

### Conceptual arrangement

```text
Main financial command topic — 4 partitions
    P0     P1     P2     P3
     \      |      |     /
      \     |      |    /
       1 Kafka Consumer instance
       (one group member, up to
        4 assigned partitions)
                 |
        Bounded work intake
                 |
       per-payment work gating
                 |
       Four-worker executor pool
       W1      W2      W3      W4
                 |
          PostgreSQL state checks
                 |
          Simulated PSP (if safe)
                 |
          Durable result / handoff
                 |
       Eligible contiguous offsets
             committed

`paymentId` is the Kafka record key.
Never run conflicting financial operations for the same payment in parallel.
```

This is a **logical design sketch**, not four independently deployed worker services or a production configuration file.

## Important consequences and safeguards

1. **Small and intentional:** four main-topic partitions support a partition-aware learning setup; one consumer instance keeps the baseline easy to operate; four workers allow us to demonstrate overlapping work for different payments.
2. **Partitions are not equal to workers:** the 4:1:4 selection is not a Kafka constraint or a throughput formula. Each worker may finish in a different order; consumer concurrency and offsets require independent handling.
3. **No hard throughput SLA:** no guaranteed messages/s, maximum latency, p95/p99, or supported number of simultaneous HTTP requests to the PSP is inferred from these values.
4. **Canonical key:** `paymentId` as lower-case hyphenated UUID in UTF-8 provides same-payment key consistency under a stable partitioner and partition count. It does **not** ensure business-operation order or control across different Kafka topics.
5. **Per-payment serialization:** a locally keyed queue/claim must avoid simultaneous PSP execution for two operations on the same Payment. **Database transaction guards and stable PSP identity** remain authoritative where processes overlap, on rebalance or delayed retries.
6. **Offset safety:** transferring a command to an **in-memory** worker queue is **not** a completed operation or recoverable handoff. Commit manual offsets only after a durable outcome or independent recovery transfer (ADR-0005). When jobs finish out of order for the same Kafka partition, commit only contiguous completed offsets.
7. **Bounded backpressure:** a maximum four actively executing jobs does not itself cap waiting tasks. We must separately select a **bounded queue/in-flight limit**, intake control and strategy for staying within consumer-poll and rebalance constraints before implementation.
8. **Retry topology is distinct:** the already agreed **single retry topic**, DLQ, persistent Retry Scheduler and Dispatcher (ADRs 0007–0009) remain in scope, but this decision does **not** assign them four partitions, one consumer, or four workers. Those technical flows may have a different topology.
9. **Local validation:** experiments should cover multiple independent payments, consecutive operations for one payment, a slow simulated PSP, a worker crash, duplicate Kafka delivery, offset completion out of order, full worker/queue capacity, and a delayed retry after a later command. The detailed benchmark matrix is deferred; no real high-scale traffic is required.

## Alternatives considered

### Size the first MVP for high-volume production traffic

**Not selected.** Extra partitions, replicas and worker infrastructure would add complexity without an evidence-based educational requirement.

### Single sequential consumer doing all PSP calls on its poll thread

**Not selected**, consistent with ADR-0020. This ties Kafka poll progress to simulated external HTTP latency and does not exercise bounded asynchronous execution.

### Four partitions, one consumer instance, four asynchronous workers

**Accepted.** Deliberately simple and sufficient as a baseline to study partition affinity, per-payment exclusion, queue backpressure, offset durability and recovery.

## Explicitly open

- **Worker intake bound:** number of pending/queued records, per-partition in-flight limits and flow control; not the same as the now-approved four executing workers.
- Actual Kafka topic names, final consumer group ID, exact producer partitioner, and schema/serializer runtime setup.
- Practical worker completion tracking, contiguous offset commits, poll-loop liveness, rebalance handling, shutdown and crash recovery.
- Distributed database claim/lease/transition safeguards and valid-but-temporarily-ineligible operation handling.
- Specific retry-topic/DLQ partition and consumer count, scheduler persistence, Dispatcher durable handoff and replay.
- Load test methodology, observable metrics and success criteria, future scaling/repartitioning procedures.
- Any change to this four-partition, one-consumer, four-worker **initial baseline** should be conscious and measured, not justified solely by hypothetical production traffic.

## Documentation-only boundary

This ADR records an accepted **local MVP starting configuration**, not an operational deployment. No Kafka topics or partition settings are created, and no application code, workers, schemas, SQL migrations or runnable benchmarks are implemented.
