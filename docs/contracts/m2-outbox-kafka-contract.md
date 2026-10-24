# M2 — Outbox Publisher & Kafka Transport Contract

- **Status:** **PROPOSED — requires explicit user approval** (2026-10-24).
- **Purpose:** Close executable M2 transport and Outbox publication details for [M2-01](../tasks/M2-01-publish-first-outbox-command.md) and [M2-02](../tasks/M2-02-recover-outbox-publication.md) while keeping both tasks **PLANNED / not Codex-authorized** until this contract and M2 scope are approved.
- **Accepted foundations (not reopened here):** [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md), [ADR-0011](../adr/0011-execute-payment-operation-command-envelope.md), [ADRs 0012–0017](../adr/0013-apache-avro-schema-registry-full-transitive.md), [ADR-0018](../adr/0018-payment-id-as-kafka-command-partition-key.md), [ADR-0019](../adr/0019-canonical-utf8-uuid-kafka-command-key.md), [ADR-0021](../adr/0021-small-mvp-kafka-partition-and-worker-topology.md) and [M1 contract](m1-payment-creation-contract.md).

## 1. M2 runtime and topics — proposed

- **Spring Boot stays on the host via IntelliJ**, using native logs/debugger/DevTools. PostgreSQL remains in the existing Docker service with its named persistent volume.
- Add **one Apache Kafka broker in KRaft single-node combined broker/controller mode** and **one Confluent-compatible Schema Registry**, both in Docker; no ZooKeeper.
- Proposal: use matching, **explicitly pinned compatible Confluent Platform image versions**, with `confluentinc/cp-kafka` and `confluentinc/cp-schema-registry`. Confirm available compatible tags when implementing; avoid `:latest`. Use health/readiness checks and a named volume for Kafka data. Development only: replication factor 1 is **not high availability or production durability**.
- Keep PostgreSQL as the default Compose service; Kafka/Registry are opt-in via a Compose profile (e.g. `messaging`) to preserve the M1-only development experience. The app connects to the host-exposed Kafka bootstrap and Schema Registry endpoints; container-internal listeners/addresses differ from host-facing ones. Do not start a consumer or require Kafka to execute M1 tests.
- Main Kafka topic: **`payment.operations.requested.v1`**, **4 partitions** (ADR-0021), **replication factor 1** (local-only), **7-day retention** (NFR-001). Do not resize partitions silently because `paymentId` partition affinity depends on stable partitioning.
- Retry-topic and DLQ **roles** are accepted in ADR-0007, but their physical topic setup, consumer group IDs, wrappers and scheduler belong to M5. **No running Payment Processor consumer in M2.** No application-managed financial result topics or domain events.

## 2. M2 command wire contract — proposed

- Value: **Apache Avro**, schema stored in the repository as a reviewable `.avsc`. Suggested Avro full name: **`com.paymentflow.commands.ExecutePaymentOperation`** (previously illustrative in ADR-0014; proposed to become final for M2).
- Schema Registry subject: exactly the **fully qualified Avro record name** via **`RecordNameStrategy`**, **`FULL_TRANSITIVE`** compatibility; do not switch to `<topic>-value` / `TopicNameStrategy`.
- Required Avro fields: `messageId`, `commandType`, `operationId`, `correlationId`, `occurredAt`; optional `causationId`. Types/defaults exactly as ADR-0015. `commandType` is literal `ExecutePaymentOperation`.
- **Proposal to settle the remaining `paymentId` question:** retain `paymentId` **only as the mandatory Kafka record key**, **not** as an Avro value field. Existing `operationId` in the value identifies the authoritative PostgreSQL PaymentOperation; value is otherwise self-contained as a logical command. `operationType`, amount, status and PSP secrets do not travel in the command.
- Kafka **key**: authoritative existing `Payment.id`, **lowercase canonical 36-character UUID, UTF-8**, without Avro key encoding or Schema Registry key subject (ADRs 0018–0019). An absent/invalid key prevents publication; do not generate or substitute an ID.
- Value encoding: Confluent-compatible Avro serializer with **writer schema identity embedded using the established schema-ID prefix wire format** (version/magic byte followed by Schema Registry schema ID and Avro payload). No application `schemaVersion`. Do not rely on optional Kafka headers for essential command identity. Schemas must be registered with the agreed subject/compatibility policy before sending; incompatible schema updates fail CI/startup rather than silently redefining the subject.
- Suggested **initial schema** (canonical structural proposal; field sequence follows ADR-0015):

```json
{
  "type": "record",
  "name": "ExecutePaymentOperation",
  "namespace": "com.paymentflow.commands",
  "fields": [
    {"name": "messageId", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "commandType", "type": "string"},
    {"name": "operationId", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "correlationId", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "occurredAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "causationId", "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": null}
  ]
}
```

- `messageId` and command fields come from the persisted **original M1 Outbox intent**, not a fresh UUID generated during each send attempt. Key `paymentId` comes from that intent and is validated against its existing PaymentOperation/Payment relation.
- The serializer may use compatible Java Avro bindings chosen by the Architect, but must reproduce this schema and the same subject. M2 will add required Avro/Schema Registry Maven dependencies; Spring for Apache Kafka is already in the starter.

## 3. Publisher durability, ownership and retry — proposed

- Read only **committed**, eligible unpublished Outbox rows. An initial M1 `PENDING` row is eligible. Implement one logical **polling Publisher** in the same Spring Boot application (not a separate microservice).
- Reserve selected rows **atomically and briefly** in PostgreSQL using a **claim with expiration/lease**; then publish to Kafka **outside the DB transaction**. Never hold the payment admission or a DB transaction open while awaiting Kafka/network I/O.
- Define a narrow delivery state lifecycle: **`PENDING → PUBLISHING → PUBLISHED`**; expired `PUBLISHING` claims are reclaimable after a crash. Track attempts, lease ownership/expiry and next eligible retry through Flyway additions to the Outbox supporting table (respecting existing M1 records). No destructive migration or replacement of the original message identity.
- Proposed local operating defaults (externalized/configurable): poll every **2 seconds**, batch max **20** rows, claim lease **60 seconds**, per-send delivery deadline **30 seconds**, publication failure retry backoff starting at **5 seconds** and capped at **60 seconds**. A lease must exceed the bounded in-flight send deadline under the configured conditions; expired claims may still cause duplicates.
- Kafka producer uses **`acks=all`**, **`enable.idempotence=true`**, producer retries constrained by delivery deadline. This limits duplicates within a producer session but **does not** solve the crash between broker acknowledgment and Outbox state update; application-level **at-least-once** persists.
- Only a **confirmed successful Kafka delivery acknowledgment** permits setting `PUBLISHED` and `published_at`. A timeout, Registry failure, broker outage, serialization failure or failed state update leaves work recoverable (`PENDING`/eligible expired `PUBLISHING`, with diagnostic retry data).
- After a successful send but **before** the database update, a crash may cause the **same `messageId`** to be republished in a separate Kafka record. Do not regenerate `operationId`, `correlationId` or `occurredAt` for such a resend.
- Multiple concurrent Publisher invocations must not silently lose work; an expired claim is recoverable. Delivery remains **at least once**, never financially exactly once. An incompatible/undecodable row is not discarded or infinitely tight-looped: retain it and expose a sanitized publish error for correction; no M5 DLQ behavior here.
- No automatic Outbox deletion/retention purge in M2. The database pending intent is durable across Kafka or Schema Registry downtime and application restart.

## 4. M2 testable deliverables — proposed

**M2-01:**
1. With Postgres/Kafka/Schema Registry local, a committed M1 admission produces a Kafka command decodable with registered writer schema and the exact accepted identities. No PSP call occurs.
2. The record's key is **exactly** the 36 UTF-8 bytes of canonical `paymentId`. Main topic has 4 partitions and 7-day retention.
3. `FULL_TRANSITIVE` compatibility is enforced on the **record-name subject**, optional `causationId=null` decodes, and incompatible schema changes are rejected.

**M2-02:**
4. Kafka or Registry down: accepted Outbox remains retryable; upon recovery it publishes **without** needing the client to resubmit payment creation.
5. Crash/restart before or after a broker ack does not strand eligible rows; a possible duplicate retains the same logical `messageId`. Published state is recorded **only** after confirmed ack.
6. Two Publisher workers/instances racing for eligible rows cannot erase work; expired claims are reclaimed; failures do not modify Payment financial status or duplicate an existing PaymentOperation.
7. Automated real-service integration tests use local/Testcontainers PostgreSQL, Kafka and Schema Registry as appropriate; no H2 replacement for PostgreSQL transaction/claim logic.

## 5. Explicit M2 exclusions and approval gate

- No Kafka Payment Processor consumer, offset commits, PSP financial calls, CAPTURE/VOID, retry-topic/DLQ consumer workflow, delayed Retry Scheduler or dispatcher, domain outcome event publication, authentication or production Kafka HA.
- The **M1 HTTP success criterion remains DB durability** and must not wait for Kafka availability.
- M2 finishes when the Publisher safely hands off commands to Kafka; it does **not** promise the command has been financially executed.
- **User approval needed** for this proposal's local runtime, named main topic/4 partitions, Avro full name/key-only `paymentId` choice, delivery claim/retry policy and automated verification. Once approved, update [M2-01](../tasks/M2-01-publish-first-outbox-command.md), [M2-02](../tasks/M2-02-recover-outbox-publication.md), the [roadmap](../roadmap.md) and the [task backlog](../planning/proposed-complete-task-backlog.md) to `APPROVED` **without starting Codex automatically**.
