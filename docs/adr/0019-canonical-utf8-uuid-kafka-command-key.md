# ADR-0019 — Canonical UTF-8 UUID Encoding for Kafka Payment Command Keys

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Kafka record-key encoding for `ExecutePaymentOperation` commands
- **Related:** [ADR-0018 — paymentId as the Kafka partition key](0018-payment-id-as-kafka-command-partition-key.md), [ADR-0017 — Avro value and optional headers](0017-avro-payload-as-command-source-kafka-headers-for-transport.md), [ADR-0014 — RecordNameStrategy for Avro values](0014-record-name-strategy-for-kafka-avro-subjects.md), [ADR-0011 — Command identity](0011-execute-payment-operation-command-envelope.md), [ADR-0004 — Transactional Outbox](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0008 — Persistent Retry Scheduler](0008-persistent-kafka-retry-scheduler.md)

## Context

ADR-0018 selected `paymentId`, the stable identity of the parent `Payment`, as the non-null Kafka record key for all `ExecutePaymentOperation` command publications. An identical *logical* UUID must always produce identical **key bytes** for a fixed topic partitioning setup. Without a canonical representation, differences in casing, hyphens, formatting or serializer selection across the Outbox Publisher and Retry Dispatcher could accidentally route the same payment to different partitions.

The remaining decision is whether the key should be the readable textual representation of a UUID or its 16-byte binary representation.

## Decision

**Serialize the `paymentId` Kafka key as a canonical, lowercase, hyphenated UUID string encoded with UTF-8. Do not Avro-encode the Kafka key.**

1. The key value is the **existing UUID** from the authoritative parent `Payment.id`, resolved from the persisted `PaymentOperation.paymentId` relationship. It must not be regenerated during publication, Kafka redelivery or dispatch of retries.
2. Render the UUID using the canonical **8-4-4-4-12**, lowercase-hexadecimal, hyphen-separated, 36-character textual form, with no surrounding quotes, braces, `urn:uuid:` prefix, whitespace or newline.
3. Encode those 36 ASCII characters as **UTF-8** bytes. Example key text: `b8d7106f-5930-4b6b-b7fb-08985070a91d`. For this canonical ASCII-only representation, the key is 36 bytes.
4. All components publishing **`ExecutePaymentOperation` command records** — including initial Outbox publication and later Dispatcher publication of the same command type — must use the **same** normalization and encoding rules. A consistent UTF-8 string serializer is a straightforward implementation candidate, not approved executable configuration.
5. Invalid or missing `paymentId` must **not** trigger a random or fallback key such as `operationId` or a null/unkeyed financial command. Publication remains pending/recoverable until the authoritative UUID can be resolved or an explicit error path handles malformed data.
6. **The Kafka value remains Apache Avro** per ADR-0013/0017, and its subject naming remains `RecordNameStrategy` per ADR-0014. The Kafka **key is plain UTF-8 text**, so no Avro key schema or Schema Registry subject is required for this chosen representation.
7. This decision does **not** require including `paymentId` in the Avro value; that question remains open. The approved Avro command value continues to use `operationId` for the stable identity of the financial operation.

## Conceptual Kafka record

```text
Kafka record
├── key:       "b8d7106f-5930-4b6b-b7fb-08985070a91d"
│              (36 lowercase ASCII characters encoded as UTF-8;
│               authoritative parent paymentId)
├── headers:   optional transport and observability metadata
└── value:     Avro ExecutePaymentOperation
               ├── messageId
               ├── commandType
               ├── operationId
               ├── correlationId
               ├── occurredAt
               └── causationId (optional)
```

The displayed quotes are for readability; **quote characters are not part of the serialized key bytes**.

## Reliability and ordering limits

- Kafka partition assignment depends on the **serialized key bytes**, selected partitioner and topic partition count. Matching UUID text and UTF-8 bytes yield consistent key input; **stable partition affinity still assumes a compatible partitioner and stable partition count**.
- Within one topic/partition, records are ordered by offset. That does not prove `AUTHORIZE`, `CAPTURE` and `VOID` are financially eligible or executed in business order.
- The main operations topic and retry topic have **independent partition logs**. A common `paymentId` key does not enforce order between them, nor does it make delayed scheduler retries sequential with newer commands.
- Re-encoding the same UUID in uppercase, omitting hyphens, or switching to binary changes the Kafka key bytes and can change partition assignment. A future key-format migration must be explicitly designed, with in-flight ordering/recovery considerations.
- The persistent Retry Scheduler may preserve the original key bytes or regenerate them from the **same persisted UUID**, as long as the exact canonical encoding contract is respected. The scheduler's durable storage format and the technical retry-topic wrapper remain undecided.
- Database state, transaction guards, stable PSP identity and idempotency remain the actual financial protections. This ADR provides **partition affinity**, not exactly-once financial processing.

## Alternatives considered

### Raw binary UUID (16 bytes)

**Not selected for the MVP.** More compact, but imposes an additional representation convention and reduces out-of-the-box readability for diagnostics. The extra bytes of a UTF-8 UUID are not currently justified as a throughput bottleneck.

### Canonical UUID string, UTF-8 (36 bytes)

**Accepted.** Easy to inspect, interoperable with common Java UUID string representations and deterministic across producers when consistently normalized.

## Explicitly deferred decisions

- Topic names, partition count, partitioner selection, expansion/migration policy, consumer groups and concurrency.
- Whether `paymentId` should also be a field in the Avro command value.
- Technical retry-message and DLQ key/record formats when they are **not** publishing `ExecutePaymentOperation` directly.
- Exact Outbox/Dispatcher persistence schema and serializer configuration, schema-identity preservation and recovery mechanics.

## Documentation-only boundary

Only Markdown architectural decisions are recorded. This ADR creates no production code, Kafka topics, broker/producer configuration, Avro `.avsc` schemas, SQL migrations or running services.
