# ADR-0012 — Full Compatibility as the Default for Kafka Command Schema Evolution

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Evolution and compatibility of the logical `ExecutePaymentOperation` Kafka command contract
- **Related:** [ADR-0010 — Generic ExecutePaymentOperation command](0010-generic-execute-payment-operation-command.md), [ADR-0011 — Approved conceptual command envelope](0011-execute-payment-operation-command-envelope.md), [ADR-0008 — Persistent retry scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0006 — Invalid-message quarantine](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0005 — Offset commit after durable work](0005-manual-kafka-offset-commit-and-recoverable-processing.md)

## Context

`ExecutePaymentOperation` is a generic logical command referring to an existing `PaymentOperation`, not a statement that a payment has succeeded. Producers, Payment Processors and Retry Scheduler/Dispatcher instances may evolve or be deployed at different times.

Furthermore, Kafka has an initial seven-day message retention policy, but messages or scheduling responsibilities may survive longer in persistent recovery storage. A new consumer may therefore encounter older command versions, including after a delayed retry is dispatched.

Changing a command schema carelessly can make legitimate, durably recorded financial-processing work unreadable, even if the current producer and consumer happen to agree.

## Decision

Adopt **Full Compatibility** as the **default policy for compatible changes** to the `ExecutePaymentOperation` command contract:

- **Backward compatibility:** a new consumer can correctly interpret command messages written by older supported producers.
- **Forward compatibility:** an older still-supported consumer can correctly interpret messages written by a newer producer.
- **Full compatibility:** both directions hold **for the versions that must coexist or remain recoverable**, without silently changing the command's financial meaning.
- **Preserve recoverability across history:** compatibility is not evaluated solely against the immediately preceding version or Kafka's retention window. Before retiring an old reader/schema, verify that no still-recoverable stored retry or record depends on it, or provide an explicit, tested migration/decoder path. This is a **lifecycle requirement**, not a choice of a particular schema-registry compatibility setting.

### Safe additive evolution

1. Add new attributes as **optional** or with a precisely defined, safe default that gives old messages valid meaning for new readers.
2. Older consumers should tolerate **unknown noncritical fields** only when the chosen serialization/validation technology can safely support this; ignoring an unknown attribute must not bypass authorization, financial validation, security or idempotency requirements.
3. Do not silently change the **type, meaning or required status** of existing fields, especially `operationId`, `messageId`, `commandType`, `correlationId` and `occurredAt` as approved in ADR-0011.
4. Do not casually remove or rename fields required by supported readers. To rename, prefer an additive migration with a transition period, while retaining the original field until supported consumers and recoverable records have safely moved.
5. Verify compatibility in **both directions** before deployment, including representatives of messages still in persistent retry storage.
6. The consumer's handling of `schemaVersion` must distinguish an understood **compatible extension** from a truly unsupported breaking contract. Merely incrementing a version number for an additive field must not automatically turn otherwise readable commands into poison messages.

## Example: adding an optional field

Conceptual prior envelope (V1):

```json
{
  "messageId": "message-001",
  "commandType": "ExecutePaymentOperation",
  "schemaVersion": 1,
  "operationId": "operation-123",
  "correlationId": "flow-789",
  "occurredAt": "2026-10-21T18:00:00Z"
}
```

Illustrative compatible extension (V2):

```json
{
  "messageId": "message-002",
  "commandType": "ExecutePaymentOperation",
  "schemaVersion": 2,
  "operationId": "operation-123",
  "correlationId": "flow-789",
  "occurredAt": "2026-10-21T18:01:00Z",
  "priority": "NORMAL"
}
```

The `priority` attribute is only a **teaching example**, **not** an approved addition to the PaymentFlow envelope or a decision to support multiple priorities.

This extension is compatible **only if** the newer consumer supplies the same safe default for missing `priority`, older consumers can safely ignore the additional field, and old readers do not reject the newer `schemaVersion` solely by an overly strict version check. A change of the version field by itself does not guarantee compatibility.

## Breaking changes

Breaking changes include (depending on schema and reader behavior): removing or renaming required `operationId`, changing its representation incompatibly, reinterpreting a required field, or introducing an attribute whose semantics must be understood to execute payments safely.

**Breaking changes do not ship under the same compatibility contract as if they were additive.** They require a consciously designed new contract/version boundary, staged rollout and an explicit, tested migration or parallel-reading strategy for older consumers and durably stored work.

The exact version naming scheme (including whether to change command type or Kafka topic for an incompatible version), release procedure and cutover/retirement criteria are **not** decided in this ADR.

## Recoverability and operational rules

| Situation | Required behavior |
| --- | --- |
| New Payment Processor consumes a command produced before its deployment | Must understand its still-supported older schema, or route it through a previously defined migration/decoder strategy. |
| Old Payment Processor consumes a newer, additive command | Must still interpret all financially material fields safely, without rejection of harmless extensions. |
| Retry Scheduler replays a durably stored older message after a deployment | Must preserve original `operationId` and financial identity; schema migration must not create another independent PSP effect. |
| Truly invalid/incompatible message | Use the durable quarantine/DLQ classification in ADR-0006; do not mark PaymentOperation as financially failed solely due to serialization errors. |
| Producer and consumer are deployed independently | Compatibility must be verified before the new producer begins publishing formats the still-running consumers cannot safely interpret. |

**Compatibility does not equal financial authorization:** parsing an event successfully never authorizes a PSP call by itself. Every worker still rechecks the persisted operation, concurrency and idempotency rules.

## Alternatives considered

### Backward compatibility only

**Not selected as the default.** Allows new readers to handle older data, but does not protect readers that are still running when a newer producer begins publishing.

### Forward compatibility only

**Not selected as the default.** Does not independently ensure that new readers can process old messages or persisted scheduled work.

### Full compatibility by default, with explicit migration for breaking changes

**Accepted.** Reduces disruption during mixed-version deployments and protects older durable retry work, while retaining a deliberate path for noncompatible changes.

## Consequences and limitations

- New features may need optional fields, defaults or staged migration rather than immediate contract replacement.
- Both producer and consumer behavior must be tested; changing a version number alone is insufficient.
- Strict reader validation must still reject data that could change financial behavior unsafely, even if unknown-field tolerance is generally enabled.
- **[ADR-0013](0013-apache-avro-schema-registry-full-transitive.md) subsequently selects Apache Avro with Schema Registry and `FULL_TRANSITIVE`** for the Kafka command family. This does not replace the semantic and historical-recoverability requirements established in this ADR.
- **A Schema Registry with `FULL_TRANSITIVE` was approved subsequently in [ADR-0013](0013-apache-avro-schema-registry-full-transitive.md)**. Registry checks only cover versions retained for a schema subject; safe old-message replay, migration, lifecycle and financial semantics require additional validation.
- Any persisted older message that remains recoverable must have a valid interpretation or explicit migration path before support is dropped.

## Open decisions

- **Resolved by [ADR-0013](0013-apache-avro-schema-registry-full-transitive.md):** Apache Avro plus Schema Registry with `FULL_TRANSITIVE`. **[ADR-0014](0014-record-name-strategy-for-kafka-avro-subjects.md) selects `RecordNameStrategy` for value subjects.** Exact registry product, Avro names/namespaces and governance implementation remain open.
- Define the exact `schemaVersion` interpretation, version compatibility checking mechanics and supported-version lifecycle.
- Define serialization of IDs/timestamps, unknown-field validation, and placement of metadata in Kafka headers versus body.
- Finalize `paymentId` placement and Kafka partition key, topic and consumer group contracts.
- Develop a test matrix and rollout/rollback criteria for compatible additions and breaking migrations.

## Documentation-only boundary

This ADR records a compatibility policy only. No production code, schema files, registry configuration, SQL migrations, Kafka topics or deployed components are created.
