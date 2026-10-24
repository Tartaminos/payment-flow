# ADR-0018 — paymentId as the Kafka Partition Key for Payment Commands

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Kafka record key and same-payment partition affinity for `ExecutePaymentOperation` publications
- **Related:** [ADR-0001 — Payment and PaymentOperation](0001-separate-payment-and-payment-operations.md), [ADR-0005 — Manual offset commits](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0007 — One retry topic and DLQ](0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008 — Persistent Retry Scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0010 — Generic ExecutePaymentOperation](0010-generic-execute-payment-operation-command.md), [ADR-0011 — Command identities](0011-execute-payment-operation-command-envelope.md), [ADR-0014 — RecordNameStrategy for Avro values](0014-record-name-strategy-for-kafka-avro-subjects.md), [ADR-0017 — Avro command value and Kafka headers](0017-avro-payload-as-command-source-kafka-headers-for-transport.md)

## Context

A `Payment` may have multiple distinct financial `PaymentOperation` records (e.g., `AUTHORIZE`, `CAPTURE`, `VOID`). The authoritative `PaymentOperation` identifies its parent `Payment` through `paymentId`.

Kafka distributes records across partitions according to their serialized key and the producer partitioning strategy. Our message contract already identifies **an individual operation** by `operationId`, while the domain must enforce rules across **all operations of the same Payment**.

The open choice was to key the command by the **operation** or its parent **payment**.

## Decision

**Use the stable parent `paymentId` as the Kafka record key for `ExecutePaymentOperation` command publications.**

1. Every `ExecutePaymentOperation` Kafka publication — including initial Outbox publication and future Retry Dispatcher republication of that command — must have its parent `paymentId` as the **non-null Kafka record key**.
2. The key is derived from the **authoritative persisted relation** `PaymentOperation.paymentId`. It must remain the same across command redelivery, technical retry, and republication of the same logical operation. For *different* `PaymentOperation` records belonging to one `Payment`, the key is still the **same** `paymentId`.
3. The producer/dispatch flow must retain or safely resolve the key when constructing its durable publication intent. If the key cannot safely be obtained, it must **not** substitute `operationId`, generate a new key, or publish an unkeyed financial command. Recover the publication according to the agreed Outbox/scheduler failure policy. The exact method of carrying the key in durable Outbox/Retry Scheduler records is a later design choice.
4. The Avro **value** still carries the accepted fields: `messageId`, `commandType`, `operationId`, `correlationId`, `occurredAt`, and optional `causationId`. **This ADR does not add `paymentId` to the Avro value**. Its presence there remains a separate, unresolved decision. Kafka key identity and business command identity have different responsibilities.
5. Keep the key's serialization **deterministic and consistent between all producers** of the same topic: using the same logical `paymentId` must yield the same serialized key bytes for a fixed partitioner/partition-count configuration. **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) subsequently approves a lowercase, hyphenated, canonical UUID string encoded as UTF-8** for the Kafka key. The exact serializer implementation/configuration, partitioner and partition count remain **open**.
6. This key choice targets **partition affinity within each topic** for commands belonging to the same payment. It is **not** a guarantee of global financial/business-operation ordering.

### Conceptual Kafka record

```text
Kafka record
├── key:      paymentId (canonical UUID text, UTF-8; ADR-0019)
├── headers:  optional transport/observability context (ADR-0017)
└── value:    Avro ExecutePaymentOperation
             ├── messageId       : UUID
             ├── commandType     : "ExecutePaymentOperation"
             ├── operationId     : UUID
             ├── correlationId   : UUID
             ├── occurredAt      : timestamp-millis
             └── causationId     : optional UUID
```

The key `paymentId` answers **which payment should share Kafka partition affinity**. The value's `operationId` answers **which already persisted financial operation might be processed**.

## Ordering and safety limits

- **Same topic, stable partitioning configuration:** records with the same serialized `paymentId` key are normally assigned to the same partition; a partition's log is ordered. This does **not** itself establish that financial actions happened in business order or that concurrently published commands arrive in an intended workflow sequence. **[ADR-0020](0020-bounded-async-payment-processing-with-per-payment-exclusion.md) subsequently approves bounded asynchronous workers with per-payment exclusion**, while database safeguards remain authoritative.
- **Different Kafka topics:** the main operations topic and retry topic have **independent logs**. Equal keys in those topics cannot impose any total order across them.
- **Persistent retries:** a delayed command can be executed after a later command has already been observed or processed; successful durable handoff and scheduler eligibility do not turn Kafka into a cross-topic transaction sequencer.
- **Partition-count/partitioner changes:** Kafka partition affinity for a fixed key can change after repartitioning or a partitioner/key-encoding change. Operational changes must preserve in-flight ordering assumptions safely; counts and expansion strategy are not set by this ADR.
- **Consumers/DB remain authoritative:** before PSP execution, a Payment Processor rechecks Payment/PaymentOperation states, database concurrency controls and idempotency/PSP identity. Valid but temporarily ineligible work must follow the accepted durable handling policy, never be blindly executed on arrival.
- **No financial exactly-once guarantee:** choosing `paymentId` does not prevent duplicates from Kafka redelivery, Outbox republishing or dispatcher crashes, and does not supersede BR-001/002/004/008 or the offset durability/reconciliation ADRs.

## Alternatives considered

### `operationId` as the Kafka record key

**Not selected.** Distributes individual operations independently, but `AUTHORIZE`, `CAPTURE` and `VOID` for the same Payment could go to unrelated partitions even within the same topic. This weakens useful per-payment affinity while cross-operation preconditions still depend on the same persisted Payment.

### `paymentId` as the Kafka record key

**Accepted.** Aligns partition affinity with the Payment aggregate and the sequence of interdependent operations while keeping the database responsible for the actual safety invariants.

## Not yet approved

**Subsequent M2 implementation contract (approved 2026-10-24):** the [M2 contract](../contracts/m2-outbox-kafka-contract.md) settles the initial main topic, four partitions and `paymentId` as **Kafka key only** (not included in the Avro value) for the `ExecutePaymentOperation` command. The originally open items below remain historical except where resolved for M2; retry-topic specialized records and broader scaling decisions remain deferred.

- Exact Kafka topic names, consumer-group identity, other topics' partitions and partition expansion/operational procedures. **[ADR-0020](0020-bounded-async-payment-processing-with-per-payment-exclusion.md) establishes the bounded asynchronous model and [ADR-0021](0021-small-mvp-kafka-partition-and-worker-topology.md) sets the starting main-topic/processor sizing to 4 partitions, 1 consumer instance and 4 workers.**
- **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) approves canonical lowercase UUID text encoded with UTF-8** for the key, not Avro. Exact serializer/configuration, topic partitioner and partition count remain open. `RecordNameStrategy` continues to govern **Avro value** subjects.
- Whether `paymentId` also appears in the Avro value. `operationId` remains the required authoritative operation lookup field.
- The single retry topic's technical record shape and whether that distinct wrapper also carries `paymentId` as its Kafka key; it must still preserve source references and remain safely recoverable. If it republishes the approved `ExecutePaymentOperation` record, this ADR's key applies.
- Durable Outbox and Retry Scheduler storage/dispatch mechanics for preserving `paymentId`, source schema identity, retry deduplication and attempts.
- Resolution strategy for valid but currently financially ineligible operations; topic keying alone does not solve delayed ordering.

## Documentation-only boundary

Only an architecture decision and documentation links are changed. No Kafka configuration, live topic, `.avsc`, SQL/migration, producer/consumer code or running infrastructure is created.
