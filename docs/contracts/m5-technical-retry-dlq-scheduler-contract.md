# M5 — Technical Retry, DLQ, Durable Scheduler & Dispatcher Contract

- **Status:** **PROPOSED — pending explicit user approval** (2026-10-24). M5's four slices remain PLANNED.
- **Authority:** [ADR-0005](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0006](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0007](../adr/0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008](../adr/0008-persistent-kafka-retry-scheduler.md), [ADR-0009](../adr/0009-exponential-backoff-with-jitter-for-kafka-retries.md), [M2 contract](m2-outbox-kafka-contract.md), [M3 proposal](m3-payment-processor-contract.md).
- **Invariant:** Technical consumer retries are **not** financial PSP retries. `UNKNOWN` is reconciled independently. Never advance an original offset before a durable outcome or independently recoverable handoff.

## Local topology and exact names

- Main operation topic remains **`payment.operations.requested.v1`** with 4 partitions and 7-day retention, canonical `paymentId` key and Avro `ExecutePaymentOperation` value.
- **One** technical retry topic **`payment.operations.retry.v1`**, **2 partitions**, replication factor 1 and **7-day Kafka retention**. Consumer group **`paymentflow-retry-scheduler-v1`**, one active consumer. One DLQ topic **`payment.operations.dlq.v1`**, **1 partition**, replication factor 1 and **7-day Kafka retention**. Local educational default, not HA.
- Retry-topic records are **distinct technical envelopes**, not the financial command schema. Retry/DLQ topics do **not** inherit main-topic ordering; only DB state and stable operation identity enforce payment correctness. Publisher/Dispatcher producing `ExecutePaymentOperation` to the main topic still use canonical `paymentId` key.
- For technical wrapper routing, use canonical **UTF-8 `retryId`** or **`quarantineId`** as wrapper record key (not a substitute for payment-level financial exclusion). Schema Registry `RecordNameStrategy` and `FULL_TRANSITIVE` apply to each distinct Avro record-name subject.

## M5-01 — Durable technical retry handoff

1. Transient examples: database unavailable **before authoritative financial result**, temporary downstream infrastructure issue and safe consumer dependency outage. **Never** put an ambiguous executed PSP financial request into Kafka technical retry as authorization to repeat POST; it goes to `UNKNOWN` + durable reconciliation instead.
2. Envelope record **`com.paymentflow.retry.PaymentOperationRetry`** (`.avsc` tracked in repository) carries: required `retryId UUID`, `sourceTopic string`, `sourcePartition int`, `sourceOffset long`, `sourceRecordKey bytes`, `sourceRecordValue bytes` (original serialized **Confluent wire bytes with schema ID**), `failureClass string`, `failedAt timestamp-millis`; optional nullable UUID `operationId`, `correlationId`, and `sourceMessageId` (default null). Kafka headers may carry telemetry but are not required for replay. Never include PAN/CVV or secrets.
3. **Retry identity is deterministic** for one failed source delivery: SHA-256 or namespace-derived UUID of `sourceTopic|partition|offset|sourceMessageId-if-known`; the same source redelivery yields the same `retryId`. Repeated technical *cycles* are different source Kafka positions and can have different retry IDs; none creates another PaymentOperation.
4. Publish retry envelope and obtain successful broker acknowledgment (**acks=all**, local replication factor 1) **before** original consumer offset becomes contiguous-eligible. Failure to publish → original offset **uncommitted**, with diagnostic/backpressure; never drop the record.
5. Retry handoff can progress only if retained source bytes/schema ID and original coordinates are sufficient for later reconstruction. A technical failure does not implicitly set `Payment:FAILED`.

## M5-02 — Poison records and recoverable quarantine

- Definitively malformed Avro, unsupported command type, corrupted wire framing or permanent schema-invalid payload are poison. **Temporary registry/broker/DB unavailability is not poison**; PSP DECLINED or UNKNOWN is not poison. A missing optional tracing header is not poison.
- Separate record **`com.paymentflow.dlq.PaymentOperationQuarantine`** with required `quarantineId UUID` (deterministic source record identity), original source topic/partition/offset, original **key/value bytes**, failure code and timestamp; optional operation/correlation/message IDs when safely decodable. Preserve original schema identity via raw Confluent value bytes.
- Acknowledge the source offset only after a durable broker ack of the DLQ publication. Deduplicate by `quarantineId` on review/replay. The **MVP must also persist a searchable PostgreSQL quarantine index/copy** (including safe original bytes and reason, uniquely keyed) so quarantined evidence is not lost merely because the Kafka DLQ record expires after 7 days. The DB copy is not permission to commit early without DLQ broker ack.
- No automatic DLQ replay; manual/controlled re-drive is allowed only by verifying original PaymentOperation state and safe business eligibility. No financial status change is inferred from malformed transport.

## M5-03 — Persistent schedule and stable retry progression

1. `scheduled_retries` supporting table (Flyway) contains unique `retry_id`, source coordinates, original key/wire value bytes, writer-schema identity, `operation_id`/correlation when known, `retry_number`, `next_eligible_at`, `state=PENDING|CLAIMED|DISPATCHED`, lease fencing/expiry, `dispatch_message_id` nullable, audit timestamps/last error.
2. The retry-topic consumer **durably inserts or idempotently finds the same retry_id** before committing its source retry-topic offset. No sleep-in-consumer design and no claim that Kafka alone delays a record.
3. **Retry number cannot reset after redelivery.** Resolve from persisted lineage of `sourceMessageId`/previous dispatcher publication to earlier schedules; an unknown provenance after DB outage must be held for diagnosis/recovery, **not treated automatically as first retry**. Record the number durably when scheduling; a duplicate Kafka delivery returns the same existing schedule/number/time.
4. Compute nominal Kafka-processing retry delay `min(300s, 10s × 2^(retry_number-1))`. Apply **uniform ±20% jitter to the capped nominal delay**, then cap the **final** result at 300s (ADR-0009; sixth and later retry delay 240–300s). `next_eligible_at` is persisted using UTC DB time and not recalculated on duplicate arrival.
5. Retry and PSP HTTP attempt counts are independent. Persisted schedule work may be delayed even if the original PaymentOperation is now terminal; its later command becomes an idempotent no-op.

## M5-04 — Durable Dispatcher and safety

- Poll due work every **2s**, max **20 schedules** per batch, **60s fenced claim lease** and **30s bounded broker delivery deadline**. Redispatch with a **new command `messageId` generated once and durably persisted for that schedule**, preserving original `operationId`, `correlationId`, canonical `paymentId` key, and `causationId` pointing to prior command publication when available. Retry of **that same dispatcher publication** reuses persisted `dispatch_message_id`.
- Prefer **transactional schedule-state update + an Outbox intent** to publish the new main-topic command, reusing M2's recoverable Publisher (never mark DISPATCHED solely on in-memory enqueue). Mark schedule durably handed off after the Outbox intent is committed; the M2 Publisher confirms Kafka separately. If sending directly, equivalent broker-ack/fenced recovery is mandatory. This contract chooses **schedule + new Outbox intent atomically** as the local default.
- Source financial command's original authoritativeness still comes from PostgreSQL; Dispatcher doesn't call PSP and must not reset attempts. Late/duplicate commands are checked against authoritative operation status and per-payment financial guards.

## Backpressure, retention and monitoring gates

- Kafka retry/DLQ retention **7 days**, not indefinite storage. Scheduled work is retained in Postgres until terminal/reconciled/explicitly closed; unresolved evidence is on retention hold. No automatic DLQ after arbitrary transient retry count, and no false promise of infinite recovery if both Kafka retention and scheduler persistence are unavailable.
- Operator metrics and alerts: **retry-ingestion oldest unpersisted age >24h (warning), >5 days (critical)**; DLQ/quarantine record count nonzero (warning); permanently failing transient dependency >1h (warning); schedule overdue >10min (warning). These are local demonstrable defaults, not production paging obligations. An ingestion outage approaching 7-day Kafka retention requires intervention, not silent offset advancement or record loss.
- Consumer transport checks validate serialized Avro with real Schema Registry, Kafka and PostgreSQL (Testcontainers or equivalent). Include repeated source record, DB outage, retry-topic/DLQ ack failure, scheduler restart, multiple dispatchers racing, persistence-before-offset, 7-day window limits and no false financial charges.

**Task order:** [M5-01](../tasks/M5-01-durable-technical-retry-handoff.md) → [M5-02](../tasks/M5-02-quarantine-poison-commands.md) / [M5-03](../tasks/M5-03-persist-technical-retry-schedules.md) → [M5-04](../tasks/M5-04-dispatch-due-technical-retries.md). The complete M5 is operationally recoverable **only after M5-03/M5-04**; do not claim a producer-only retry topic has infinite durable recovery.

**Approval gate:** Scope and technical contract are proposed; M5 task statuses stay PLANNED until the user explicitly accepts them. No code is initiated by this document.
