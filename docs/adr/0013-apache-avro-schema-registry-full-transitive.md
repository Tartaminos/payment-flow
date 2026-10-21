# ADR-0013 — Apache Avro and Schema Registry with FULL_TRANSITIVE Compatibility

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Serialization format, schema governance, and compatibility enforcement for PaymentFlow Kafka commands
- **Related:** [ADR-0010 — Generic ExecutePaymentOperation command](0010-generic-execute-payment-operation-command.md), [ADR-0011 — Command envelope and identity](0011-execute-payment-operation-command-envelope.md), [ADR-0012 — Full Compatibility policy](0012-full-compatibility-for-kafka-command-schema-evolution.md), [ADR-0004 — Outbox and asynchronous processing](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0008 — Persistent retry scheduler](0008-persistent-kafka-retry-scheduler.md)

## Context

PaymentFlow uses Kafka to deliver the generic `ExecutePaymentOperation` command for existing, durable financial operations. Producers, consumers, a persistent Retry Scheduler and a Dispatcher may deploy independently. Retried commands can outlive the initial seven-day Kafka retention because the Scheduler preserves durable recovery work.

[ADR-0012](0012-full-compatibility-for-kafka-command-schema-evolution.md) approved **Full Compatibility** as the conceptual contract-evolution policy. The system now needs a concrete serialization technology and a mechanism for validating schema changes before incompatible command messages enter payment processing.

JSON Schema, Apache Avro and Protocol Buffers were compared with Java/Spring Boot, Kafka integration, evolution safety, diagnostics and operational complexity in mind. Message size alone is **not** the deciding factor: the generic command is relatively small.

## Decision

**Use Apache Avro to serialize PaymentFlow's Kafka command payloads, governed through a Schema Registry with `FULL_TRANSITIVE` schema compatibility for the appropriate command schema subject(s).**

1. **Apache Avro** is the accepted serialization/schema technology for `ExecutePaymentOperation` Kafka commands. This decision does not commit all future, unrelated message families to Avro without separate evaluation.
2. Use a **Schema Registry** that supports Avro and the `FULL_TRANSITIVE` compatibility mode (as defined by the Confluent-compatible schema-registry model). The exact product and deployment strategy remain open. **[ADR-0014](0014-record-name-strategy-for-kafka-avro-subjects.md) subsequently accepts `RecordNameStrategy` for Kafka Avro value subjects**; final Avro names/namespaces and environment governance remain open.
3. **`FULL_TRANSITIVE` checks backward and forward schema compatibility against all earlier schema versions still registered under a subject**, not merely the immediate preceding version. The compatibility setting must be applied at the right subject scope and enforced for schema evolution.
4. Keep the existing [ADR-0011](0011-execute-payment-operation-command-envelope.md) conceptual envelope: `messageId`, `commandType`, `schemaVersion`, `operationId`, `correlationId`, `occurredAt` required; `causationId` optional. Whether to include `paymentId`, how to encode UUID/timestamps, and whether individual fields are in Kafka headers vs. the Avro payload remain open.
5. The message's business `schemaVersion` field (if retained in its exact present form) and **Schema Registry schema IDs/versions** are distinct concepts. Their concrete relationship, and how producers select/read schema identities on the wire, require a separate contract design decision.
6. **Schema-valid does not mean financially valid.** The Payment Processor always reads the authoritative `PaymentOperation` and its `Payment` from PostgreSQL and applies state, concurrency, idempotency and PSP-identity rules before any financial HTTP call.
7. Persisted retries and replay paths must remain readable under [ADR-0012](0012-full-compatibility-for-kafka-command-schema-evolution.md). Do not retire/delete old schemas or decoder support while recoverable stored work may still depend on them unless an explicit, tested migration path is ready.

## Conceptual transport responsibilities

```text
Payment API transaction -> PaymentOperation + audit + Outbox
                                           |
                                           v
                                    Outbox Publisher
                                           |
                             Avro schema / registry identity
                                           |
                                           v
                            Kafka command message (Avro)
                                           |
                                           v
                                    Payment Processor
                                           |
                          Read Avro + validate durable state
                                           |
                                           v
                            PSP action only if financially safe

Transient consumer failure -> approved retry topic -> persistent scheduler
                               -> dispatcher -> command with same operationId
Incompatible / malformed command -> approved durable DLQ/quarantine
Ambiguous PSP financial result     -> UNKNOWN + independent reconciliation
```

The paths and responsibilities remain those accepted in ADRs 0004–0011. This decision chooses **serialization and schema governance** only; it does not add a new domain-event topic or guarantee exactly-once PSP effects.

## Compatibility and recovery requirements

- Schema registration/evolution must reject incompatible changes under the selected `FULL_TRANSITIVE` subject policy. For Avro, field additions and removals need deliberate defaults/schema-resolution behavior; not every field addition, rename or type change is automatically safe.
- **Transitive checks cover schemas retained within the registry subject**. They do not test financial semantics, cross-subject interoperability, unregistered historical schemas or formats whose registry history has been erased.
- Test old-writer/new-reader and new-writer/old-reader combinations, including samples of retried messages that may remain durably stored after Kafka's retention horizon.
- An unsupported message schema is a **message-processing problem**, not a payment rejection. Route truly unprocessable records to durable quarantine/DLQ with the original safe diagnostic context under ADR-0006; keep its associated PaymentOperation unchanged unless independently justified.
- If the registry is unavailable and a producer or consumer cannot obtain the schema needed for safe processing, do not invent a result or acknowledge incomplete processing. Preserve durable Outbox intent, retry responsibility, or Kafka offset recovery, as appropriate. Local schema caches may affect immediate availability; no assumption of guaranteed cache effectiveness is made.
- A DLQ/replay workflow must retain or recover enough original schema identity and safe message content to diagnose and deserialize older messages. Exact DLQ bytes/envelope and registry-availability procedures are deferred.
- Neither Avro nor Schema Registry replaces transactional Outbox publication reliability, consumer idempotency, PSP provider-safe retries, audit retention or financial reconciliation.

## Alternatives considered

### JSON Schema

**Not selected for the initial Kafka command contract.** Offers human-readable payloads and approachable validation, but it requires disciplined defaults/unknown-field behavior and does not provide the chosen Avro writer/reader schema-resolution model.

### Protocol Buffers

**Not selected for the initial Kafka command contract.** Offers strongly typed, compact cross-language contracts and fits gRPC particularly well. Those benefits do not outweigh the Avro-specific Kafka/schema-evolution learning goals in this project.

### Apache Avro + Schema Registry / `FULL_TRANSITIVE`

**Accepted.** Provides a compact schema-driven payload format with schema-resolution rules and transitive compatibility checks against registered historical versions.

## Consequences and trade-offs

**Benefits:** explicit schemas, a registry-backed compatibility gate, support for evolving Kafka consumers, and protection for still-recoverable older command versions.

**Costs:** registry availability and lifecycle become operational dependencies; binary messages require schema-aware tooling for human inspection; schema registration, subject naming, test coverage and safe schema retirement need governance.

**Limits:** a registry passes or fails *schema-level* checks only. A schema-compatible message can still be semantically wrong, stale, duplicated or financially unsafe.

## Open decisions — not silently approved

- **Field types/defaults were decided in [ADR-0015](0015-avro-logical-types-and-optional-command-fields.md):** UUID logical types, `timestamp-millis`, and optional `causationId` via nullable union with `default: null`. Final Avro namespace/name, validation and physical schema remain open.
- Kafka payload versus headers layout; handling and possible redundancy of `schemaVersion` alongside registry-assigned identifiers.
- **Subject naming was settled in [ADR-0014](0014-record-name-strategy-for-kafka-avro-subjects.md): `RecordNameStrategy` for Avro Kafka values**, with `FULL_TRANSITIVE` per fully qualified record name. Final record names/namespaces, registry vendor/hosting/security/configuration and producer/consumer schema resolution are still open.
- Topic names and boundaries, partition key (`paymentId` remains only a candidate), partition count and consumer groups.
- Retry Scheduler storage representation (serialized original bytes vs. reconstructed references), safe replay and schema retention/migration.
- Exact DLQ wire format and how original payload/schema identity is retained for diagnostics and later redrive.
- Domain event publication is **not** added by this decision; only `ExecutePaymentOperation` Kafka commands are in scope.

## Documentation-only boundary

This ADR documents the accepted architecture. **No `.avsc` schema, registry configuration, producer/consumer code, Kafka infrastructure, migrations or executable services are created.**
