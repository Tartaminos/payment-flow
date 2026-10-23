# ADR-0005 — Manual Kafka Offset Commit After Durable Processing or Recoverable Handoff

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Payment Processor Kafka consumption, offset acknowledgment and crash recovery
- **Related:** [ADR-0004 — Synchronous acceptance and asynchronous processing](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [BR-002 — Uncertain PSP outcomes](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-005 — Reconciliation](../requirement/BR-005-reconciliation-after-24h-unknown.md), [BR-007 — Bounded PSP attempts](../requirement/BR-007-bounded-psp-retries.md), [BR-008 — Conflicting financial operations](../requirement/BR-008-block-conflicting-financial-operations.md)

## Context

PaymentFlow accepts and durably persists payment requests synchronously, then publishes operation requests from a Transactional Outbox to Kafka for processing by a Payment Processor.

Kafka delivery must be treated as **at least once**. A consumer can crash, lose partition ownership in a rebalance, or be given a message again. Kafka committing an offset tracks **consumer-group progress**, not whether a financial effect was performed exactly once.

We must prevent two opposite problems:

- **Premature progress:** committing an offset before durably recording the outcome/recovery handoff can strand an operation.
- **Indefinite partition blockage:** waiting for a final PSP outcome on an operation that is truly `UNKNOWN` can stop unrelated records in the same partition indefinitely.

## Accepted decision

**The Payment Processor uses explicitly managed (manual) Kafka offset commits after the corresponding message's processing has produced a durable state or a durable, independently recoverable handoff.**

1. Do **not** rely on periodic auto-commit as proof that an external financial operation was processed.
2. An offset is eligible for commit once the consumer has safely established one of these durable outcomes:
   - A **definitive outcome** (`SUCCEEDED`, `REJECTED` or `FAILED` as defined by [ADR-0003](0003-distinguish-financial-success-from-rejection.md)) and its associated Payment/audit changes are committed; or
   - An **uncertain outcome** (`UNKNOWN`) is committed while preserving the last confirmed Payment state **and** recovery/reconciliation is durably registered and will proceed independently of replaying that Kafka record; or
   - A duplicate/no-op is demonstrably already handled or durably delegated to a recoverable processing workflow, with no new PSP financial effect initiated.
3. A merely in-memory decision, HTTP response from the PSP not yet persisted, or an operation left `PROCESSING` with **no independently recoverable handoff** is **not** sufficient to commit.
4. Kafka offset commits and PostgreSQL state commits are distinct actions, not one atomic transaction. If PostgreSQL succeeds but the offset commit fails or crashes, the message can be replayed: the consumer must handle that replay idempotently.
5. Where multiple messages from a partition are processed concurrently, commit only the **highest contiguous fully eligible offset range**. A completed later record cannot justify skipping an earlier outstanding record.
6. Do not use consumer offset acknowledgment as an authorization to start another independent financial operation. The original `PaymentOperation` identity remains stable under redelivery, worker failover and provider-safe retries.

**Kafka offset terminology:** committing offset `N + 1` records that the next position to consume is `N + 1` after safely handling the record at offset `N` (subject to contiguous processing).

## Conceptual lifecycle and failure cases

| Case | Correct behavior | Commit offset? |
| --- | --- | --- |
| PSP definitively authorizes a payment; database transaction records `AUTHORIZED` / `AUTHORIZE: SUCCEEDED` plus audit | Message processing is durably complete | **Yes**, after DB commit |
| PSP declines authorization; `Payment: DECLINED` / `AUTHORIZE: REJECTED` plus audit committed | Definitive business rejection is durably known | **Yes**, after DB commit |
| PSP confirms technical non-execution; `PaymentOperation: FAILED` and audit committed | Definitive technical result is durably known | **Yes**, after DB commit |
| PSP times out after safe retries; `UNKNOWN` and independently recoverable reconciliation are durably recorded | The financial outcome remains unknown, but recovery does not depend on replaying the same Kafka record | **Yes**, after durable handoff |
| PSP says `CAPTURED`, but worker crashes before storing the result | Persisted outcome may be stale; external PSP effect may exist | **No commit based on that transient response**; redelivery must resolve the **same** operation without blind recapture |
| Worker crashes or loses its partition lease while operation is `PROCESSING` | Another worker must not assume the financial operation was never sent; confirm original identity/status or safely transfer to durable recovery | **Not yet**, unless a separate durable recovery process already owns it |
| Consumer receives a duplicate for a definitively handled operation | Verify identity and previous durable result; skip any new PSP financial effect | **Yes**, once the duplicate is safely recognized |
| Kafka offset commit fails **after** a successful DB transaction | Message may be delivered again and must be idempotently recognized | Replay is safe; try to commit again |
| A record is definitively malformed or unprocessable | Transfer to durable DLQ/quarantine under [ADR-0006](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md); preserve identity and evidence without inventing a financial outcome | **Only after** quarantine is durably confirmed and safely recoverable |

### Example: timeout on an in-flight capture

```text
Kafka: CAPTURE requested (operationId = 456)
  -> Worker reserves the existing logical PaymentOperation 456
  -> PSP response is ambiguous / HTTP timeout
  -> Only contract-safe retries are considered, retaining same PSP identity
  -> Database commits:
       Payment = AUTHORIZED (last confirmed fact)
       PaymentOperation 456 = UNKNOWN
       Durable audit + independent reconciliation responsibility
  -> Consumer can commit Kafka progress
  -> Reconciliation later resolves operation 456; no independent capture is created
```

Reconciliation is **not** a reason to mark the payment as failed or canceled after the 24-hour threshold. That threshold is for escalation ([BR-005](../requirement/BR-005-reconciliation-after-24h-unknown.md)).

## Scope boundaries and caveats

- Outbox Publisher delivery acknowledgment is a **different** concern from Payment Processor **consumer offset commits**. Both require idempotency, but they are not interchangeable.
- Kafka ordering is **within a partition**. Preserving the processing order of the external financial effects, or the validity of business state transitions, still requires database/domain safeguards.
- A consumer-group rebalance can cause redelivery. Merely having one active consumer per partition does **not** provide exactly-once external financial execution.
- Keeping `PROCESSING` over a long PSP call may require leases/heartbeats, worker recovery and consumer-poll-liveness techniques. The concrete design is not approved by this ADR.
- At-least-once ingestion plus idempotency and reconciliation is the target. This is **not** a claim of end-to-end exactly-once payments.

## Kafka topology candidates — not yet approved

The following remain **proposals**, introduced for discussion but **not established by this offset decision**:

| Concern | Candidate | Status |
| --- | --- | --- |
| Topic | `payment.operations.requested.v1` | Proposal |
| Partition key | Stable `paymentId`, canonical UUID UTF-8 | **Approved subsequently in ADRs [0018](0018-payment-id-as-kafka-command-partition-key.md) and [0019](0019-canonical-utf8-uuid-kafka-command-key.md)** |
| Consumer group | `payment-processors` | Proposal |
| Partition count / consumer replicas | **4 main-topic partitions / 1 consumer instance / 4 async workers** | **Approved subsequently in [ADR-0021](0021-small-mvp-kafka-partition-and-worker-topology.md)**; retry/DLQ topology and queue bound remain undecided |
| Offset acknowledgment | Manual after durable outcome or independently recoverable handoff | **Approved by this ADR** |
| Processing semantics | At-least-once with idempotency and reconciliation | **Approved assumption/contract** |

A timestamp varying per event should **not** be appended to the proposed partition key when per-payment partition affinity is desired; that would change the key and may distribute one payment across partitions.

## Further decisions needed

- Final topic naming/boundaries and message envelopes; key serialization and initial main-topic sizing were decided in ADRs 0018–0019 and **[0021](0021-small-mvp-kafka-partition-and-worker-topology.md)**. Exact topic names, consumer-group ID, other topics' partition counts, detailed intake limits and future scaling remain open.
- **[ADR-0020](0020-bounded-async-payment-processing-with-per-payment-exclusion.md) subsequently approves bounded asynchronous work off the Kafka poll thread and per-payment exclusion.** Offset commit tracking after asynchronous completions (always contiguous and durable), rebalance callbacks, poll loop and backpressure/worker mechanics still require detailed design.
- Exact durable recovery mechanism for abandoned `PROCESSING` operations (lease expiry, status-check worker, scheduler, or other).
- The trigger for `UNKNOWN` and the durable reconciliation work record/job; ability to recover after a crash even if no Kafka record is replayed.
- Poison-message classification and durable DLQ/quarantine (versus financial reconciliation) are approved in [ADR-0006](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md). [ADR-0007](0007-single-kafka-retry-topic-and-dlq.md) subsequently approved a **single consumer retry topic and a single DLQ**, with durable forwarding before offset acknowledgment. Exact DLQ schema, delayed retry scheduling, redrive and monitoring/backpressure remain open.
- Concrete retry intervals between PSP requests (timeout budgets **5s/10s/20s** are already accepted).
- No SQL, code, Kafka configuration or running services are created by this document.
