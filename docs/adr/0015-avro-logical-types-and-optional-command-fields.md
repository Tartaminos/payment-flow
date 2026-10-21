# ADR-0015 — Avro Logical Types and Optional Fields for ExecutePaymentOperation

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Field representation for the first `ExecutePaymentOperation` Avro command schema
- **Related:** [ADR-0010 — Generic command](0010-generic-execute-payment-operation-command.md), [ADR-0011 — Command envelope](0011-execute-payment-operation-command-envelope.md), [ADR-0012 — Full Compatibility policy](0012-full-compatibility-for-kafka-command-schema-evolution.md), [ADR-0013 — Avro + Schema Registry](0013-apache-avro-schema-registry-full-transitive.md), [ADR-0014 — RecordNameStrategy](0014-record-name-strategy-for-kafka-avro-subjects.md)

## Context

PaymentFlow approved a compact logical `ExecutePaymentOperation` command envelope (ADR-0011), Apache Avro with a Schema Registry and `FULL_TRANSITIVE` compatibility (ADR-0013), and `RecordNameStrategy` (ADR-0014). We now need to specify **how identifiers, publication time, and optional causal metadata are represented by Avro**, without silently approving the Kafka partition key, registry schema-ID lifecycle, or header-versus-value layout.

## Decision

Adopt the following **conceptual Avro field representations** for the first command contract:

| Field | Required? | Accepted Avro representation | Meaning |
| --- | --- | --- | --- |
| `messageId` | Yes | `{"type":"string","logicalType":"uuid"}` | One logical message publication; see ADR-0011. |
| `commandType` | Yes | `"string"` | Expected command discriminator: `ExecutePaymentOperation`; validate its value. |
| `operationId` | Yes | `{"type":"string","logicalType":"uuid"}` | Original, stable financial operation reference. |
| `correlationId` | Yes | `{"type":"string","logicalType":"uuid"}` | Correlation across the request and processing flow. |
| `occurredAt` | Yes | `{"type":"long","logicalType":"timestamp-millis"}` | Instant this command publication was created. |
| `causationId` | No | `["null", {"type":"string","logicalType":"uuid"}]`, **`"default":null`** | Optional preceding causal message identity. |

The required fields deliberately **do not get fabricated fallback defaults**. A missing identifier must not be silently replaced by a randomly generated or empty identifier during schema resolution. A nullable `causationId` has `null` as the first union branch, matching its default.

The accepted `uuid` logical type is carried by Avro `string`; the `timestamp-millis` logical type is carried by Avro `long` and denotes milliseconds since the Unix epoch for an absolute instant. Neither encodes a localized timezone label. Java type conversion details (e.g., `UUID` and `Instant`) depend on the selected serializer and bindings, which remain implementation decisions.

## Conceptual Avro record representation

The following block reflects the current field set as amended by [ADR-0016](0016-remove-redundant-command-schema-version.md) and belongs **only in this Markdown ADR**. It demonstrates the approved field choices but is **not an authorized `.avsc` file** or registered production schema. The namespace `com.paymentflow.commands` is **illustrative**, not a finalized naming commitment.

```json
{
  "type": "record",
  "name": "ExecutePaymentOperation",
  "namespace": "com.paymentflow.commands",
  "fields": [
    {
      "name": "messageId",
      "type": { "type": "string", "logicalType": "uuid" }
    },
    {
      "name": "commandType",
      "type": "string"
    },
    {
      "name": "operationId",
      "type": { "type": "string", "logicalType": "uuid" }
    },
    {
      "name": "correlationId",
      "type": { "type": "string", "logicalType": "uuid" }
    },
    {
      "name": "occurredAt",
      "type": { "type": "long", "logicalType": "timestamp-millis" }
    },
    {
      "name": "causationId",
      "type": ["null", { "type": "string", "logicalType": "uuid" }],
      "default": null
    }
  ]
}
```

The actual Kafka wire representation is **binary Avro**, not this JSON declaration. The Schema Registry schema identifier/versioning mechanism and Kafka record metadata still need design.

## Compatibility implications

- `FULL_TRANSITIVE` remains the approved policy **within each fully qualified Avro record subject** under `RecordNameStrategy`.
- A new **optional** field can be introduced using an Avro-compatible union/default, subject to explicit registry compatibility checks and application-level behavior tests.
- A field default participates in **reader/writer schema resolution** when a field is absent in an *older writer schema*. It does **not** mean a producer can omit that field arbitrarily when writing a record whose own writer schema requires it.
- Older readers may ignore compatible newly written fields, but **Avro schema compatibility is not proof of business correctness**. Changes to `operationId`, financial meaning, identity semantics or validation are high-risk even when the schema is accepted.
- `commandType` stays a string for the single accepted generic command, not an Avro enum: an unexpected value remains a validation concern and must never be interpreted as authorization to perform a different PSP operation.
- **[ADR-0016](0016-remove-redundant-command-schema-version.md) subsequently removes `schemaVersion` from this command.** Schema Registry writer-schema identity now governs structural versioning; do not recreate the removed field without a separately approved business-semantic purpose.
- `operationType` is not needed in the command envelope: the database owns the authoritative `PaymentOperation.type`. `paymentId` remains undecided in the payload and as a Kafka partition-key candidate.
- The financial execution rules remain governed by the Payment/PaymentOperation records, transaction guards, provider idempotency and reconciliation, **not** by schema validation.

## Alternatives considered

### Plain strings and human-readable timestamps

**Not selected for the initial schema.** They can be decoded without logical-type-aware conversion, but convey less precise semantic intent in the Avro schema than UUID/timestamp logical types.

### Mandatory causation ID or synthetic default identifiers

**Not selected.** The first publication may have no causal parent message; generating a fake identity to satisfy a schema could harm financial traceability.

### UUID logical types, millisecond instant, nullable causal ID

**Accepted.** Gives the minimal command envelope explicit semantics while supporting compatible absence of causation metadata.

## Explicitly open

1. **Resolved by [ADR-0016](0016-remove-redundant-command-schema-version.md):** remove the redundant `schemaVersion` from the Avro command payload; use Schema Registry identity for structural versions. A distinct business-semantic version remains unapproved.
2. Placement of metadata in Kafka **headers vs. Avro value**, including whether all listed fields stay in the value.
3. Final Avro namespace and record name, UUID lexical validation and Java binding configuration.
4. `paymentId` as payload attribute and/or Kafka record **partition key**, key serialization and partition count.
5. Physical `.avsc` artifact, registration/compatibility tests, exact publication wire envelope and registry implementation.
6. Technical retry-record Avro schema and Scheduler/Dispatcher persistent handoff contract.

## Documentation-only boundary

This ADR documents approved field-representation semantics in Markdown only. It creates no `.avsc` file, Kafka topic/configuration, application code, database migration, Schema Registry registration or deployed service.
