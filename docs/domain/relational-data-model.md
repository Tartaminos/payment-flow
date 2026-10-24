# PaymentFlow — Conceptual Relational Data Model (MVP)

- **Status:** Accepted conceptual baseline; physical schema, migrations and API contracts not implemented
- **Date:** 2026-10-20
- **Scope:** Credit-card payments via simulated PSP
- **References:** [State machines](payment-state-machines.md), [ADR-0001](../adr/0001-separate-payment-and-payment-operations.md), [ADR-0002](../adr/0002-append-only-audit-and-retention.md), [ADR-0003](../adr/0003-distinguish-financial-success-from-rejection.md), [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0008](../adr/0008-persistent-kafka-retry-scheduler.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md)
- **Important:** This is **documentation**, not executable SQL, application code, or a commitment to a concrete migration.

## Summary and cardinality

Four principal tables:

1. `payments`: current, confirmed financial state of one logical payment.
2. `payment_operations`: distinct **logical PSP financial operations**, not each HTTP attempt.
3. `idempotency_keys`: client-facing request identities and replay/conflict resolution.
4. `payment_audit_events`: append-only financial and operation timeline.

Relationships:
- An external order belonging to a merchant can have **multiple Payments** (e.g., an attempt declined, followed by another payment attempt with a different card).
- Each Payment belongs to one merchant (merchant registry/authentication model is deferred).
- One Payment has **many PaymentOperations** and **many audit events**.
- For the MVP, **at most one successful authorization** and **one successful full capture** per Payment; partial/multiple captures are out of scope.
- A logical PSP operation may be attempted via multiple safe HTTP requests with one stable PSP idempotency identity.
- Client idempotency associations reference the logical Payment and, where applicable, the PaymentOperation; their exact lifetime is still to be decided.

## 1. `payments`

| Column | Proposed PostgreSQL type | Meaning / constraint |
| --- | --- | --- |
| `id` | `UUID` | Primary key. |
| `merchant_id` | `UUID` | Required merchant identifier; `merchants` table is not part of the MVP baseline. |
| `external_order_id` | `VARCHAR(100)` | Required identifier of the e-commerce order; **not unique**, even per merchant. |
| `amount_minor` | `BIGINT` | Required, `> 0`; integer minor currency units, never float/double. |
| `currency` | `CHAR(3)` | Required ISO-style currency code, e.g. `BRL`. |
| `status` | `VARCHAR(30)` | Required `PaymentStatus` value. |
| `version` | `BIGINT` | Required, default `0`; optimistic concurrency version. |
| `created_at` | `TIMESTAMPTZ` | Required creation time. |
| `updated_at` | `TIMESTAMPTZ` | Required most recent change. |

Accepted Payment values: `CREATED`, `AUTHORIZED`, `CAPTURED`, `DECLINED`, `CANCELED`, `EXPIRED`.

**Do not** add a unique constraint on `(merchant_id, external_order_id)`: the same order can be paid through multiple legitimate Payment attempts, but a particular idempotent API request must not duplicate its Payment.

The expiration of an **authorization** belongs to the `AUTHORIZE` operation, not necessarily the root Payment record.

## 2. `payment_operations`

| Column | Proposed PostgreSQL type | Meaning / constraint |
| --- | --- | --- |
| `id` | `UUID` | Primary key; stable logical operation identity. |
| `payment_id` | `UUID` | Required foreign key to `payments.id`. |
| `type` | `VARCHAR(20)` | Required: `AUTHORIZE`, `CAPTURE` or `VOID`. |
| `status` | `VARCHAR(20)` | Required operation status; see six-value list below. |
| `amount_minor` | `BIGINT` | Required, `> 0`; the amount of the operation in minor units for the MVP. |
| `psp_idempotency_key` | `VARCHAR(150)` | Stable identity across safe retries; unique **within the PSP/provider scope**; exact provider scoping still pending. |
| `psp_reference` | `VARCHAR(150)` | Nullable, external PSP operation reference. |
| `psp_result_code` | `VARCHAR(100)` | Nullable provider business decision/rejection reason. |
| `error_code` | `VARCHAR(100)` | Nullable technical error/diagnostic reason. |
| `attempt_count` | `INTEGER` | Required, non-negative, default `0`; counts financial HTTP attempts for this logical operation. |
| `last_attempt_at` | `TIMESTAMPTZ` | Nullable. |
| `next_retry_at` | `TIMESTAMPTZ` | Nullable; schedule to be specified. |
| `unknown_since` | `TIMESTAMPTZ` | Nullable; used for the 24-hour reconciliation escalation. |
| `authorization_expires_at` | `TIMESTAMPTZ` | Nullable; PSP authorization validity, applicable to `AUTHORIZE` operations. |
| `started_at` | `TIMESTAMPTZ` | Nullable. |
| `completed_at` | `TIMESTAMPTZ` | Nullable. |
| `created_at` | `TIMESTAMPTZ` | Required. |
| `updated_at` | `TIMESTAMPTZ` | Required. |

Accepted operation statuses: `PENDING`, `PROCESSING`, `SUCCEEDED`, **`REJECTED`**, `FAILED`, `UNKNOWN`. By [ADR-0003](../adr/0003-distinguish-financial-success-from-rejection.md), `SUCCEEDED` means the **financial operation actually succeeded**. An authorization declined by the PSP uses `REJECTED`, not `SUCCEEDED`; `psp_result_code` stores the specific reason.

The configured PSP request **timeout budgets** are 5s / 10s / 20s for at most **three total attempts**, with safe, stable PSP operation identity; retry exhaustion without authoritative outcome remains `UNKNOWN` ([BR-007](../requirement/BR-007-bounded-psp-retries.md)).

`attempt_count` is a summary field. Individual technical attempt timing and outcomes are retained through diagnostic/audit events according to [NFR-001](../requirement/NFR-001-transaction-traceability-and-retention.md); a separate attempt table has **not** been approved.

### Logical operation invariants

- A Payment may have many operations, but at most **one successful** `AUTHORIZE` and **one successful** **full** `CAPTURE`.
- A new **independent conflicting** operation must not be admitted while a prior one is `PENDING`, `PROCESSING` or `UNKNOWN` ([BR-008](../requirement/BR-008-block-conflicting-financial-operations.md)).
- One status pre-check in application code is **not** adequate for concurrent admission: reserve the operation atomically in the database before the PSP request, with appropriate locking/conditional update and database uniqueness protection.
- An index unique per payment and operation type **only when `status = SUCCEEDED`** is one possible physical defense for successful authorization/full capture; this alone does not block concurrent in-flight actions or prove an external financial side effect was not duplicated.
- `SUCCEEDED` with `type = AUTHORIZE` identifies an approved authorization; `REJECTED` can represent a definitive card decline. `FAILED` needs proof of technical non-execution; `UNKNOWN` may require reconciliation.
- Provider identity must be explicit in the future PSP contract and reflected in scoping of `psp_reference` and `psp_idempotency_key`; do **not** assume provider-wide global uniqueness across integrations.
- On confirmed authorization expiry, new capture is prohibited and Payment can transition to `EXPIRED` ([BR-003](../requirement/BR-003-expired-authorizations.md)).

## 3. `idempotency_keys`

| Column | Proposed PostgreSQL type | Meaning / constraint |
| --- | --- | --- |
| `id` | `UUID` | Primary key. |
| `merchant_id` | `UUID` | Required identity boundary. |
| `operation_scope` | `VARCHAR(50)` | Required, e.g. `PAYMENT_CREATE`, `PAYMENT_CAPTURE`. |
| `key` | `VARCHAR(150)` | Required client-provided idempotency key. |
| `request_hash` | `VARCHAR(64)` | Required hash of canonical business request parameters (e.g. SHA-256). |
| `payment_id` | `UUID` | Foreign key to existing Payment; nullability during initial reservation is a design detail. |
| `payment_operation_id` | `UUID` | Nullable foreign key where applicable. |
| `created_at` | `TIMESTAMPTZ` | Required. |
| `expires_at` | `TIMESTAMPTZ` | Nullable; **M1 keeps idempotency entries without automatic expiry/purge**, per [approved M1 contract](../contracts/m1-payment-creation-contract.md). Future expiry policy open. |

Accepted uniqueness boundary: **(`merchant_id`, `operation_scope`, `key`)**. Equivalent repeated requests resolve to the original logical Payment/PaymentOperation; same key with different canonical request parameters is a conflict.

**M1 decisions, approved 2026-10-24:** the [M1 contract](../contracts/m1-payment-creation-contract.md) defines SHA-256 of canonical validated `orderId`, integer `amount` and uppercase `currency`, as well as no automatic expiry/purge in the local M1. The eventual expiry/replay-after-expiry policy remains open for later milestones. Five-year audit retention does not automatically dictate idempotency retention.

## 4. `payment_audit_events`

| Column | Proposed PostgreSQL type | Meaning / constraint |
| --- | --- | --- |
| `id` | `BIGINT` | Primary key; technical audit record identity. |
| `event_id` | `UUID` | Unique event identity for deduplication and tracing. |
| `payment_id` | `UUID` | Required foreign key to Payment. |
| `payment_operation_id` | `UUID` | Nullable foreign key to a specific operation. |
| `entity_type` | `VARCHAR(30)` | Whether the transition concerns Payment or PaymentOperation. |
| `event_type` | `VARCHAR(100)` | The meaningful business/operation event type. |
| `previous_status` | `VARCHAR(30)` | Nullable: previous relevant entity status, if applicable. |
| `new_status` | `VARCHAR(30)` | Nullable: new relevant entity status, if applicable. |
| `reason_code` | `VARCHAR(100)` | Nullable business or technical rationale. |
| `source` | `VARCHAR(50)` | API, worker, PSP webhook, reconciliation, etc. |
| `correlation_id` | `UUID` | Correlates a distributed flow. |
| `causation_id` | `UUID` | Nullable triggering event/command ID. |
| `sequence_number` | `BIGINT` | Per-Payment logical sequence; atomically allocated, unique per Payment. |
| `metadata` | `JSONB` | Sanitized, minimal auxiliary metadata; no sensitive PSP payload dumps. |
| `occurred_at` | `TIMESTAMPTZ` | Event time. |
| `recorded_at` | `TIMESTAMPTZ` | Database persistence time. |

The current Payment/PaymentOperation state change and its audit event must be committed in **one local database transaction**. Append new records instead of overwriting audit history. Five-year financial/audit retention is governed by [NFR-001](../requirement/NFR-001-transaction-traceability-and-retention.md); unresolved cases must not lose required evidence.

The `sequence_number` should be unique per Payment and allocated under appropriate concurrency control; wall-clock timestamps alone do not establish causal or commit order across distributed components.

Audit events and integration/outbox messages have distinct purposes. Kafka's seven-day message retention must not determine audit availability.

## Integration tables: decision and scope

The **four principal tables above remain the approved core domain model**. Subsequent [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md) approved the **Transactional Outbox** as a required logical persistence mechanism for reliable, asynchronous Kafka publication:

| Additional table | Status | Responsibility |
| --- | --- | --- |
| `outbox_events` | **Pattern accepted (ADR-0004), M1 minimal persistence contract approved** in [M1 contract](../contracts/m1-payment-creation-contract.md); publisher/delivery mechanisms are later milestones. | Persist publication intent in the **same database transaction** as accepted Payment/PaymentOperation/idempotency/audit changes, so a committed payment cannot lose its processing intent on a subsequent Kafka failure. |
| `psp_webhook_inbox` | **Candidate for future design**; not yet approved. | Deduplicate and process PSP callbacks safely if/when the PSP webhook contract is designed. |
| `scheduled_retries` | **Persistent scheduling responsibility accepted (ADR-0008)**; table name and PostgreSQL physical schema **not yet approved**. | Candidate to durably track consumer-retry eligibility, attempt metadata, status and deduplication; distinct from PSP financial HTTP retries and financial reconciliation. |

The Outbox and proposed retry schedules are **supporting infrastructure data**, not additional independent payment-domain aggregates. Outbox rows differ from durable `payment_audit_events` records and can be published more than once after crashes. The retry schedule is also not a substitute for PaymentOperation or PSP reconciliation. Exact topics, payloads, reliable dispatch strategy, retention, and implementation remain open.

No SQL, runtime services, event payloads or infrastructure are created by this document.

## Explicitly pending (not silent implementation commitments)

- The physical database schema, migration syntax, concrete check constraints/indexes, transaction and lock design, error handling for concurrent admission.
- Whether the authorization/capture *attempt* identity needs more structure when retries and multiple providers are introduced.
- `merchant_id` validation without a local merchant table, multi-tenant boundary enforcement.
- PSP provider scoping, definitive result-code taxonomy, whether `psp_idempotency_key` must be immediately non-null before dispatch.
- Future idempotency-key expiry/purge policy remains open; **M1** no-expiry key policy, canonical hash fields, and unique/transactional concurrent admission are approved in the [M1 contract](../contracts/m1-payment-creation-contract.md).
- Exact behavior and eventual terminal status of technical failures after a Payment remains `CREATED`.
- Detailed event ordering, audit retention start, and storage optimization.
- Retry intervals between attempts (distinct from the agreed **request timeouts** 5s / 10s / 20s).

This specification intentionally does **not** contain executable SQL or application code.
