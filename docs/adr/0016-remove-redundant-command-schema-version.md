# ADR-0016 — Remove Redundant schemaVersion from ExecutePaymentOperation

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Kafka command envelope simplification and schema-version ownership
- **Amends:** [ADR-0011 — Command envelope](0011-execute-payment-operation-command-envelope.md), [ADR-0015 — Avro field representations](0015-avro-logical-types-and-optional-command-fields.md)
- **Related:** [ADR-0010 — Generic command](0010-generic-execute-payment-operation-command.md), [ADR-0012 — Full Compatibility](0012-full-compatibility-for-kafka-command-schema-evolution.md), [ADR-0013 — Apache Avro and Schema Registry](0013-apache-avro-schema-registry-full-transitive.md), [ADR-0014 — RecordNameStrategy](0014-record-name-strategy-for-kafka-avro-subjects.md), [ADR-0008 — Persistent retries](0008-persistent-kafka-retry-scheduler.md)

## Context

The initial conceptual `ExecutePaymentOperation` envelope in ADR-0011 included a mandatory integer `schemaVersion`, later represented as Avro `int` in ADR-0015. Subsequently, PaymentFlow approved Apache Avro with Schema Registry, `FULL_TRANSITIVE` compatibility and `RecordNameStrategy` (ADRs 0012–0014).

Keeping an application-maintained structural schema version in the payload alongside the schema identity used by the Registry would duplicate responsibility and could introduce ambiguity if values diverge.

**No Avro `.avsc` artifact, registered schema, running producer/consumer or actual Kafka data exists yet** under this documentation-only project milestone. Therefore, the decision can be revised at the design stage without a live-message migration. This is not a general recommendation to remove required fields from an already deployed `FULL_TRANSITIVE` schema: doing so could break compatibility with old readers.

## Decision

**Remove `schemaVersion` from the logical and Avro `ExecutePaymentOperation` command payload. Rely on the Schema Registry's schema identity and compatibility history for structural versioning.**

- The **Schema Registry** is authoritative for registered Avro schema identity and version history. Under the intended Confluent-compatible serialization flow, the schema ID carried with serialized data identifies the writer schema; the exact serializer/wire envelope remains a separate implementation detail.
- `RecordNameStrategy` groups schema history by fully qualified Avro record name; the selected **`FULL_TRANSITIVE`** policy is enforced per relevant subject.
- Neither producers nor consumers should require a separate numeric **`schemaVersion` field in the command** solely to decide how to deserialize it.
- **No replacement custom structural version property** is introduced in headers or payload by this ADR.
- If command semantics later require a distinct **business-protocol version** beyond Avro structural identity, design and approve it as a separate, explicitly meaningful contract. Do not silently reuse a registry version or infer financial permissions from schema identity.

## Revised conceptual command envelope

| Field | Presence | Avro representation / role |
| --- | --- | --- |
| `messageId` | Required | `string` with `logicalType: uuid`; one logical publication identity. |
| `commandType` | Required | `string`; the generic command discriminator `ExecutePaymentOperation`. |
| `operationId` | Required | `string` with `logicalType: uuid`; stable existing PaymentOperation identity. |
| `correlationId` | Required | `string` with `logicalType: uuid`; flow trace identity. |
| `occurredAt` | Required | `long` with `logicalType: timestamp-millis`; command publication creation instant. |
| `causationId` | Optional | Nullable `null` / `string` logical `uuid` union with default `null`. |
| `paymentId` | Not decided | Still a possible Kafka record key and/or payload field, subject to a later decision. |
| `schemaVersion` | **Removed** | Not part of the command contract; structural versions are managed by Schema Registry. |

The following example shows the **revised logical content**. JSON is for explanation; Kafka command serialization uses **Avro**:

```json
{
  "messageId": "c13a7544-1f79-45f4-9312-8993319e7a1d",
  "commandType": "ExecutePaymentOperation",
  "operationId": "b8d7106f-5930-4b6b-b7fb-08985070a91d",
  "correlationId": "91d0d856-3e2a-4df5-bba6-c92797d26db6",
  "occurredAt": "2026-10-21T18:00:00Z",
  "causationId": null
}
```

The `causationId` may be omitted from a **conceptual JSON depiction**, but the actual Avro writer-record value must conform to its writer schema; `default:null` is primarily used for reader/writer schema resolution, not blanket permission to omit fields in serialized records.

## Identity, compatibility and financial safety

1. **Publication identity != financial identity.** `messageId` may change for a deliberately republished retry, but `operationId` remains the original financial operation identifier and `correlationId` continues the trace.
2. A Schema Registry **schema ID is not itself a business-operation version**, PSP operation identity, payment state or authorization to execute a financial effect.
3. Removing a field **after deployment** is not necessarily a compatible Avro schema change. Any future breaking schema evolution must follow ADR-0012, including old-reader/new-writer testing, controlled rollout and migration of still-recoverable retry work.
4. The persistent Retry Scheduler must retain sufficient schema identity/record information or reliable references to deserialize older work, even if Kafka's original record is no longer retained. Registry lifecycle management must account for that.
5. A schema-valid command still requires the Payment Processor to validate authoritative Payment/PaymentOperation state, concurrency and PSP idempotency safeguards before considering external financial execution.

## Alternatives considered

### Keep payload `schemaVersion` and Registry versions

**Not selected.** Duplicates structural version metadata without an approved, separate business-semantic purpose, risking contradictory version identifiers.

### Remove `schemaVersion`, use Registry schema identity for structure

**Accepted.** Retains the already agreed Avro/Schema Registry governance while simplifying the command contract.

## Open decisions

- Final Avro record name/namespace, physical `.avsc` layout and validation rules.
- Kafka key (`paymentId` candidate), partitioning and consumer groups. **[ADR-0017](0017-avro-payload-as-command-source-kafka-headers-for-transport.md) subsequently establishes the Avro value as the authoritative command and headers as optional transport/observability metadata**; exact optional header names remain open.
- How writer schema IDs and registry subjects are resolved and preserved in the Outbox, persistent Retry Scheduler, DLQ and controlled redrive paths.
- Whether a future business-semantic protocol version is required; **none is currently approved**.
- Registry implementation, operational configuration and migration/retention of historical schemas.

## Documentation-only boundary

This ADR changes **documentation and architectural intent only**. It does not create code, Avro files, Kafka topics, Schema Registry registrations, SQL migrations or runtime services.
