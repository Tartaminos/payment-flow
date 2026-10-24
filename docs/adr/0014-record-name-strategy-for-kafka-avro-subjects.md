# ADR-0014 — RecordNameStrategy for Avro Schema Registry Subjects

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Avro Schema Registry subject naming for Kafka command records
- **Related:** [ADR-0007 — Single Kafka retry topic and DLQ](0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008 — Persistent Retry Scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0010 — Generic ExecutePaymentOperation command](0010-generic-execute-payment-operation-command.md), [ADR-0011 — Command envelope and identity](0011-execute-payment-operation-command-envelope.md), [ADR-0012 — Full Compatibility](0012-full-compatibility-for-kafka-command-schema-evolution.md), [ADR-0013 — Apache Avro, Schema Registry and FULL_TRANSITIVE](0013-apache-avro-schema-registry-full-transitive.md)

## Context

PaymentFlow adopted Apache Avro and a Schema Registry with `FULL_TRANSITIVE` compatibility for its Kafka command contracts (ADR-0013). The same logical `ExecutePaymentOperation` command can be initially published by the Outbox Publisher and later republished by the Retry Dispatcher while preserving its `operationId`.

We must decide whether schema compatibility history belongs to the **Kafka topic** or to the **Avro record type**. Topic-based subjects can allow diverging versions of an otherwise identical command type when used in several topics. Type-based subjects instead make the command's schema evolution consistent wherever that type is published.

## Decision

**Use `RecordNameStrategy` for Avro Schema Registry value subjects in the PaymentFlow Kafka command family.**

- A Subject is derived from the Avro record's **fully qualified name** (namespace plus record name), rather than from the Kafka topic name.
- Any Kafka topic publishing the **same fully qualified Avro record type** uses the **same subject and schema evolution history** for that record.
- Enforce the already approved **`FULL_TRANSITIVE`** policy **on each relevant record-name subject**. This checks registered schema versions in both compatibility directions across the retained subject history. A change compatible within one subject is **not** automatically compatible with unrelated record subjects.
- **Different responsibilities use different Avro record types**, each with its own subject. The generic financial execution command is conceptually distinct from any future technical retry, scheduling or quarantine record.
- This decision does **not** finalize the Avro record namespace, concrete schema, actual topic names, physical retry-message envelope or serialization of the Kafka message key.

### Illustrative subjects (candidate names, not approved schema names)

| Record purpose | Illustrative Avro fully qualified name / Subject | Status |
| --- | --- | --- |
| Generic command to process an already persisted PaymentOperation | `com.paymentflow.commands.ExecutePaymentOperation` | **Logical command approved in ADR-0010**; qualified Avro name is an example only |
| Optional dedicated technical retry record, if selected later | `com.paymentflow.retry.PaymentOperationRetry` | **Record shape and even the need for a distinct wrapper remain open**; name is only illustrative |

If `ExecutePaymentOperation` is present in more than one topic, its **same record-name subject** governs each publication. If the retry topic instead contains a **different technical Avro record**, that record has its **own** schema subject and independent version history; its references to `operationId`, original publication identity and recovery context must preserve prior idempotency guarantees.

## Illustration

```text
Schema Registry: value subjects identified by Avro record type

Subject: com.paymentflow.commands.ExecutePaymentOperation
  ├── V1
  ├── V2
  └── V3                <-- FULL_TRANSITIVE within this subject
         |
         +-- Main operation request publication
         |
         +-- Later dispatch of the same command record
              (if dispatched to a topic carrying that record)

Possible separate subject, only if a distinct retry record is designed:
Subject: com.paymentflow.retry.PaymentOperationRetry
  ├── V1
  └── V2                <-- separate FULL_TRANSITIVE history
```

The diagram shows logical relationships, not a final choice to serialize the retry topic using either type or to route dispatcher output to a particular topic.

## Consequences and safety constraints

1. **Consistent evolution by type:** a schema update to a command record is checked against that record's registered history, independent of the topics that carry it.
2. **No cross-record compatibility promise:** `FULL_TRANSITIVE` is applied to **each** record-name subject, not across all Avro records in the project.
3. **Shared evolution is intentional:** two topics cannot intentionally evolve incompatible versions of the *same fully qualified record name* independently under this strategy; if responsibilities differ, design meaningfully distinct records instead of reusing the same name.
4. **A Kafka topic may contain multiple Avro record types** under record-based subject naming. If this is ever used, consumers must deliberately support or safely reject the allowed types; it does not authorize mixing arbitrary records in a topic.
5. **Schema compatibility is structural, not financial.** The Payment Processor still validates the authoritative PostgreSQL PaymentOperation/Payment before any PSP call. No duplicate financial effect is permitted merely because two records have different message IDs or schema versions.
6. **Persistent retry recovery:** older records kept by the Retry Scheduler must remain decodable even after the source Kafka records expire. Registered schema history, available reader schemas, correct original schema identification and operational migration paths remain critical; `FULL_TRANSITIVE` is insufficient if needed schemas are deleted or their semantic meaning changed.
7. **Malformed or unrecognized messages:** an undecodable record follows the established durable DLQ/quarantine procedure; it is not proof that a payment was rejected or failed.
8. **Keys are separate:** value subject naming is independent of the Kafka partition key, which remains to be chosen (with `paymentId` previously discussed as a candidate). **[ADR-0018](0018-payment-id-as-kafka-command-partition-key.md) subsequently chooses `paymentId` as the record key for financial commands**; **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) subsequently selects canonical UUID text in UTF-8 for the key, with no Avro key schema required**.

## Alternatives considered

### `TopicNameStrategy`

**Not selected for the command record.** Straightforward when each topic carries one value schema, but schemas for an identically named logical command in different topics would have separate compatibility histories (e.g., `<topic>-value`).

### `TopicRecordNameStrategy`

**Not selected.** Gives each topic-and-record pair its own schema history. Useful for deliberate topic-specific evolution, but adds multiple independently governed subjects for the same command type.

### `RecordNameStrategy`

**Accepted.** Centers compatibility on the Avro record's identity rather than its route through Kafka, matching the project's goal of a stable, reusable generic command.

## Explicitly open

**Subsequent M2 implementation contract (approved 2026-10-24):** the [M2 contract](../contracts/m2-outbox-kafka-contract.md) finalizes command record/subject `com.paymentflow.commands.ExecutePaymentOperation`, `FULL_TRANSITIVE`, mandatory Kafka `paymentId` key only, and the main topic `payment.operations.requested.v1`. The older open questions below are historical to this ADR; retry wrappers and M5 topic formats are still open.

- **Field representations were accepted in [ADR-0015](0015-avro-logical-types-and-optional-command-fields.md):** UUID logical types, `timestamp-millis`, nullable `causationId` with a default of `null`, and no artificial defaults for required fields. Final namespace/record name and physical `.avsc` contract are still open.
- Registry implementation/provider, subject naming governance across environments, authorization/credentials and registration procedures.
- **[ADR-0016](0016-remove-redundant-command-schema-version.md) removes `schemaVersion` from the command.** Exact Schema Registry schema-ID resolution and retention remain to be designed.
- Header-versus-payload fields and exact serialization envelope, including placement (or omission) of `paymentId`.
- **[ADR-0018](0018-payment-id-as-kafka-command-partition-key.md) chooses `paymentId` for the financial command key.** **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) settles key encoding**; Kafka topic names, partitioner/count, allowed record types per topic and consumer groups remain undecided.
- Whether the single retry topic carries the original command record, a distinct retry wrapper, or another durable recoverable representation; Scheduler ingestion and Dispatcher handoff contracts.
- DLQ format, schema identity preservation and controlled replay; safe removal/archival of historical subjects and schemas.

## Documentation-only boundary

This ADR records a design decision only. No `.avsc` files, services, SQL/migrations, Schema Registry settings, Kafka topics or runnable code are created.
