# ADR-0017 — Avro Payload as Command Contract; Kafka Headers for Transport Metadata

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Responsibility boundary between the Avro message value, Kafka headers, and Kafka record key
- **Related:** [ADR-0004 — Transactional Outbox](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Durable Kafka offset commits](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0008 — Persistent Retry Scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0010 — Generic ExecutePaymentOperation command](0010-generic-execute-payment-operation-command.md), [ADR-0011 — Command identity and envelope](0011-execute-payment-operation-command-envelope.md), [ADR-0013 — Avro and Schema Registry](0013-apache-avro-schema-registry-full-transitive.md), [ADR-0015 — Avro logical field types](0015-avro-logical-types-and-optional-command-fields.md), [ADR-0016 — Remove redundant schemaVersion](0016-remove-redundant-command-schema-version.md)

## Context

PaymentFlow has approved a generic `ExecutePaymentOperation` Kafka command referencing an existing, persisted `PaymentOperation`. Its Avro field representations were accepted in ADR-0015, and ADR-0016 eliminated the application-level structural `schemaVersion` in favor of the Schema Registry's schema identity.

A Kafka record has separate **key**, **value** and **headers**. We must decide where the command's semantically required identifiers and timestamp live. Separating essential identity across Kafka headers and an Avro value would make recovery from a persisted command harder, particularly after technical retries, scheduler persistence, republishing and DLQ review. Kafka headers are also outside the chosen Avro value subject's `FULL_TRANSITIVE` compatibility checks.

## Decision

**The Avro value is the self-contained, authoritative logical `ExecutePaymentOperation` command contract. Kafka headers are reserved for supplementary transport, diagnostics and observability context.**

- The **Avro command value** contains the approved logical command fields: required `messageId`, `commandType`, `operationId`, `correlationId` and `occurredAt`, plus optional `causationId` (nullable with `default: null`).
- The **Kafka headers** may carry complementary technical metadata (e.g., distributed-tracing propagation or other diagnostic context), but **no header is required to establish the command's financial identity or interpret its business intent**. Exact header names, content, serialization and whether any will actually be emitted are **not approved by this ADR**.
- Financial processing never trusts headers over the authoritative Avro command and persisted PostgreSQL state. If an identifier is duplicated into a future observability header, that copy is **non-authoritative**; mismatch detection and diagnostic handling must be designed rather than silently using it to choose a different financial operation.
- **No mandatory command field is moved exclusively to a Kafka header**. This enables processing/reconstruction of command context using its Avro value and the writer schema identity needed for decoding, even if nonessential headers are absent.
- The **Kafka record key is a separate concern**, not a header or an Avro-value replacement. **[ADR-0018](0018-payment-id-as-kafka-command-partition-key.md) subsequently selects `paymentId` as the key for `ExecutePaymentOperation` command records**, with **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) subsequently specifying canonical lowercase UTF-8 UUID encoding**; whether to include it in the Avro value remains open.
- **Schema Registry identity is separate from business metadata.** The Avro serializer/wire format must provide sufficient writer schema information to deserialize a record; ADR-0016 removed `schemaVersion` from the command. The exact on-wire schema-ID mechanism is still an implementation decision.
- `operationType`, PSP idempotency identity, financial status and amount remain **authoritative in PostgreSQL**, not in transport metadata or the command value.
- Retry attempts, error diagnostics and scheduling times are **technical workflow metadata**. They must be stored/recovered under the approved persistent scheduler and retry contracts; this ADR does not require those attributes to be inserted into the financial command or rely on ephemeral Kafka headers as their only durable store.

## Logical record example

The following is an **illustrative representation**, not an approved JSON wire encoding or a final configuration:

```text
Kafka record
├── key:            paymentId (ADR-0018; canonical UUID UTF-8 per ADR-0019)
├── headers:        optional trace/transport metadata (exact list TBD)
└── value:          Avro: ExecutePaymentOperation
    ├── messageId:      uuid
    ├── commandType:    "ExecutePaymentOperation"
    ├── operationId:    uuid
    ├── correlationId:  uuid
    ├── occurredAt:     timestamp-millis
    └── causationId:    nullable uuid, default null

Writer schema identity: made available through the selected Schema Registry
serialization scheme; exact mechanics TBD.
```

The command identifies **which existing financial operation may be considered for execution**, not whether any PSP operation has succeeded.

## Durability, retries and observability

1. **Outbox:** the durable publication intent must preserve the command fields and any schema resolution information required by its publication strategy. It cannot rely on tracing headers as the sole record of operation identity.
2. **Kafka redelivery:** the same command publication retains `messageId` even if headers used by instrumentation differ in downstream handling.
3. **Retry Dispatcher republication:** a new logical publication may have a new `messageId` while retaining `operationId` and `correlationId`, as required by ADR-0011. Technical headers can be refreshed/propagated as appropriate but never create a new financial operation.
4. **Persistent Retry Scheduler:** stored work must retain enough authoritative references and schema context for safe recovery; losing optional diagnostic headers must not make it impossible to determine the original `PaymentOperation`. Whether the scheduler stores full Avro bytes, references, or a separate technical retry record remains open.
5. **Missing optional tracing headers:** may degrade end-to-end observability, but must not cause the processor to invent business identifiers or financial outcomes.
6. **Kafka offsets and poison messages:** missing essential fields or undecodable Avro content are handled under prior durable-processing/DLQ rules; tracing-header absence alone is not a reason to classify a valid financial command as malformed.
7. **Security:** no card credentials, raw PSP secrets, sensitive provider payloads or tokenized secrets are added to headers or the Avro command. Technical diagnostic metadata must be safe for the relevant retention and access policies.

## Alternatives considered

### Split authoritative command identifiers between value and headers

**Not selected.** Makes command interpretation depend on out-of-band fields and complicates schema-governed evolution, retries and replay.

### Put every possible observability field into the Avro command

**Not selected.** Couples business command schema evolution to transport/instrumentation details that have no bearing on the financial operation.

### Self-contained Avro command, optional technical Kafka headers

**Accepted.** Aligns with schema governance and persistent replay while keeping transport concerns distinct from financial identity.

## Open decisions

- **[ADR-0018](0018-payment-id-as-kafka-command-partition-key.md) selects `paymentId` as the Kafka command key.** **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) subsequently chooses canonical UUID text in UTF-8**, not Avro, for key bytes. Partitioner/count/operational changes and whether to duplicate `paymentId` in the Avro value remain open.
- Final Avro record name/namespace, physical schema definition and validation.
- The actual optional Kafka headers, naming/encoding and propagation rules (including tracing), plus duplicate-field mismatch treatment.
- Wire schema identity/serializer mechanics, schema caching, registry outage recovery and retained-schema lifecycle.
- Retry topic's exact record shape, persisted scheduler identity/representation, dispatcher durable handoff, DLQ schema and replay.
- Topic/group boundaries, consumer concurrency and out-of-order recovery.

## Documentation-only boundary

This ADR documents architectural intent only. It creates no running Kafka topics, Avro `.avsc` files, schema registrations, SQL/migrations, Java code, producer configuration or live services.
