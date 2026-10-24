# ADR-0022 — Bounded Per-Payment Waiting Queue

- **Status:** Accepted
- **Date:** 2026-10-24
- **Related:** [ADR-0005](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0020](0020-bounded-async-payment-processing-with-per-payment-exclusion.md), [ADR-0021](0021-small-mvp-kafka-partition-and-worker-topology.md), [BR-008](../requirement/BR-008-block-conflicting-financial-operations.md)

## Context

The PaymentFlow educational MVP has four main-topic partitions, one Kafka consumer instance and four asynchronous workers. Multiple operations relating to the same payment must not execute conflicting financial actions simultaneously. Local contention alone is not a technical infrastructure failure and should not necessarily route a command to Kafka's durable retry pipeline.

## Decision

**Keep commands already received for a busy `paymentId` in a bounded, per-payment FIFO waiting queue, not in automatic Kafka technical retry solely because the local worker is occupied.**

- Enforce at most one actively processing financial command per `paymentId` in the local worker scope; different payments can run concurrently.
- Bound both per-payment waiting and total pending/in-flight work. Exact limits, fairness and backpressure/poll details remain open.
- A queued command is not automatically financially eligible: validate authoritative PostgreSQL Payment/PaymentOperation state and PSP idempotency immediately before execution. BR-008 blocks new conflicting operation admission while a prior operation is unresolved.
- A queue entry exists **only in memory**. Never commit a Kafka offset because a job was queued or dispatched to a worker. Commit only after a durable result/recoverable handoff, and only through contiguous eligible offsets per partition (ADR-0005). Kafka redelivery must remain safe after process failure.
- When limits are reached, apply controlled intake backpressure, without dropping polled records or creating unbounded queues. Exact `pause()`, `resume()`, poll, rebalance and record bookkeeping are separate decisions.
- Kafka infrastructure failures follow the approved technical retry topic and persistent scheduler. Uncertain PSP outcomes require `UNKNOWN` and durable reconciliation, **not** indefinite in-memory waiting.
- A process-local queue/lock does not secure concurrent attempts across processes, consumer rebalances or delayed retry topics. Database concurrency guards and stable PSP idempotency remain mandatory.
- The decision retains the small four-partition, one-consumer, four-worker MVP configuration and introduces no new service.

## Example

```text
Payment A: A1 running -> bounded FIFO [A2, A3]
Payment B: B1 running independently
On A1 completion: recheck A2 against PostgreSQL before any PSP request
```

These represent received commands, not approval to create a new conflicting PaymentOperation while A1 is unresolved.

## Alternatives

- Always route a busy payment to Kafka retry: rejected as overcomplication for healthy local contention.
- Unbounded queues: rejected due to memory and fairness risks.
- Bounded per-payment FIFO with global backpressure: **accepted**.

## Open issues

Per-payment and global queue limits, worker fairness, treatment of full queues and already fetched Kafka records, offset tracking, rebalance/recovery mechanics, long-wait policy, and durable deferral of business-ineligible commands.

## Documentation-only boundary

No runtime code, Kafka settings, migrations, `.avsc` files or infrastructure are created.
