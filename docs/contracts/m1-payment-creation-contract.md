# M1 — Payment Creation API, Idempotency and Persistence Contract

- **Status:** **APPROVED** on 2026-10-24 for PaymentFlow M1, including the API 202/400/409 contract, stable resource identifiers, SHA-256 request canonicalization, no automatic key expiry in M1, PostgreSQL transaction/Outbox shape, and Testcontainers PostgreSQL integration tests.
- **Purpose:** close the implementation details not already settled in [M1](../milestones/m1-payment-creation-idempotency.md); this is the **approved M1 implementation contract** subordinate to accepted business requirements and ADRs. It authorizes the two approved M1 vertical-slice implementation tasks when deliberately invoked through the local orchestration skill, not automatic execution.
- **Authoritative limits:** [BR-001](../requirement/BR-001-idempotent-payment-requests.md), [BR-008](../requirement/BR-008-block-conflicting-financial-operations.md), [ADR-0001](../adr/0001-separate-payment-and-payment-operations.md), [ADR-0002](../adr/0002-append-only-audit-and-retention.md), [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md), [ADR-0011](../adr/0011-execute-payment-operation-command-envelope.md), [ADR-0023](../adr/0023-configured-single-merchant-for-educational-mvp.md), [relational model](../domain/relational-data-model.md).

## 1. HTTP contract — accepted for M1

```http
POST /payments
Content-Type: application/json
Idempotency-Key: checkout-150-attempt-1

{"orderId":"150","amount":50000,"currency":"BRL"}
```

- A **required** `Idempotency-Key` is a case-sensitive, nonblank printable-ASCII string of 1–150 characters; no silent whitespace normalization.
- `orderId`: required string, 1–100 characters; nonblank, no leading/trailing whitespace. It maps to `payments.external_order_id` and **is not globally unique**.
- `amount`: required **positive integer** fitting PostgreSQL `BIGINT`, expressed in the currency's minor units. Reject decimals, floats, numeric strings, zero and negative values.
- `currency`: required **three uppercase ASCII letters** (e.g., `BRL`). This is syntax validation only; it does **not** promise PSP support for every three-letter code.
- Only these three business fields are accepted; unexpected body fields should produce a documented validation error, rather than silently influencing downstream financial behavior.
- The merchant is **not provided by the client**. Read one stable synthetic UUID from local configuration as decided in ADR-0023. M1 property: `paymentflow.merchant-id`; configured synthetic value: `550e8400-e29b-41d4-a716-446655440000`. Fail startup clearly if absent/malformed. There is no authentication/merchant registry in M1.

After a **successful database transaction**:

```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "paymentId": "0e1bc578-4965-4d0e-8e3a-eecfa184a1b5",
  "operationId": "0a9a7eba-725a-4b89-8123-d2b335da8d7f",
  "status": "CREATED",
  "operationStatus": "PENDING"
}
```

No financial success is implied. During M1, there is no background PSP processing. When the same request is replayed, return **202 with the same Payment and PaymentOperation identities** (and their current persisted statuses), not a new set of records.

M1 error envelope (no stack traces, card details or SQL diagnostics):

```json
{"code":"IDEMPOTENCY_KEY_CONFLICT","message":"The idempotency key is already used for different payment details."}
```

- `400 Bad Request` / `INVALID_PAYMENT_REQUEST`: missing/invalid required header or JSON body.
- `409 Conflict` / `IDEMPOTENCY_KEY_CONFLICT`: same scoped key, different canonical business parameters.
- Unexpected infrastructure/persistence failure: **never 202**; return an appropriate server error without falsely declaring durable acceptance. No special error code or retry timing is approved here.

## 2. Idempotency — accepted for M1

- Scope: **(`merchant_id`, `operation_scope=PAYMENT_CREATE`, `key`)**, case-sensitive, with an enforced PostgreSQL unique constraint. `merchant_id` remains in the key even though only one merchant is configured initially.
- Equivalence: validated `orderId`, integer `amount` in minor units, and uppercase `currency` only; request property order and insignificant JSON formatting cannot affect equivalence. Compute a stable SHA-256 from an unambiguous canonical serialization of those three validated typed values, in fixed field order. Do not hash raw request JSON or include volatile identifiers/timestamps.
- Same key + same canonical parameters: return original payment/operation status without inserting another Payment, PaymentOperation, audit admission, idempotency row or Outbox intent.
- Same key + different canonical parameters: **409**, no mutation or additional side effects.
- Concurrent first submissions with the same scoped key: PostgreSQL uniqueness/transaction coordination must ensure **exactly one durable winner**. After that transaction commits, an equivalent contender resolves to the winner's resource; differing parameters resolve to 409. Do not return 202 for an uncommitted reservation, and do not let an application-only check race against a second insert.
- Retention **accepted for local M1**: keep idempotency associations **without automatic expiry or purge**; `expires_at` may be NULL. This avoids unsafe reuse of a retired key while a Payment remains relevant. Expiry/purge and replay-after-expiry must be explicitly designed before enabling them in a future milestone.

## 3. Transaction, core schema and Outbox — accepted for M1

One short PostgreSQL transaction atomically writes **five** records for each first valid payment request:

1. `payments`: `CREATED`, amount/currency/order ID, configured merchant, version 0.
2. `payment_operations`: one `AUTHORIZE:PENDING` for the same amount, `attempt_count=0`, stable `operationId`. The PSP idempotency identity must remain stable for this operation; M1 local identity is the canonical `operationId` UUID string. Provider-specific scoping before actual dispatch is reserved for M3.
3. `idempotency_keys`: the scoped key, canonical request hash, Payment and PaymentOperation references, `expires_at=NULL` for M1.
4. `payment_audit_events`: at least one immutable admission fact (`PAYMENT_CREATED`, `source=API`, `new_status=CREATED`, Payment/Operation IDs, correlation ID, per-Payment sequence 1); no sensitive request data. Append-only and unique `(payment_id,sequence_number)`.
5. `outbox_events`: **pending** `ExecutePaymentOperation` publication referencing the already persisted PaymentOperation, not a financial success event.

Use Flyway-managed PostgreSQL constraints consistent with the [conceptual model](../domain/relational-data-model.md): PKs, FKs, `NOT NULL`, positive amount, valid status/type values, uniqueness of the scoped idempotency key and audit sequence. Do **not** impose uniqueness on `(merchant_id,external_order_id)`. The outbox is supporting persistence, not a fifth independent payment domain aggregate.

Minimal M1 `outbox_events` fields: `message_id UUID` (unique publication ID), `payment_id UUID`, `operation_id UUID`, `command_type=ExecutePaymentOperation`, `correlation_id UUID`, `causation_id UUID NULL`, `occurred_at TIMESTAMPTZ`, `status=PENDING`, `created_at TIMESTAMPTZ`, `published_at TIMESTAMPTZ NULL`. A producer-retry of **the same** publication must preserve `message_id`; Kafka wire schema, publisher/retry lifecycle and additional publication bookkeeping belong to M2.

The Outbox contains no cardholder data or real credentials. **No Kafka producer/consumer or PSP HTTP call in M1**, despite their libraries already being in the `pom.xml`.

On any failure during the five writes, roll back **all**. Never acknowledge 202 without the commit. On replay, do not insert an additional audit creation event or Outbox row.

## 4. Runtime and independent verification — accepted for M1

- Baseline already committed: Maven, Java 21, Spring Boot 4.1.2, Spring Data JDBC, PostgreSQL 17, Flyway, Spring Web MVC, Validation and Docker Compose Support.
- Local application/debugger in IntelliJ; PostgreSQL container plus named persistent volume. Kafka broker **not needed** to start or test M1.
- Use real PostgreSQL in persistence/integration tests (use test-scoped **Testcontainers PostgreSQL** with JUnit/Spring tests; add the dependencies required for these integration tests to the `pom.xml` during the approved implementation). No H2 substitute for PostgreSQL-specific uniqueness, transactions, locks or SQL checks.
- Independent tests must cover new accepted request, equivalent and conflicting replay, concurrent duplicate submission, transaction rollback/no partial rows, restart durability where practical, and no PSP/Kafka side effects. Follow the approved skill's Architect → Coder → Test Engineer gates.

## Explicit exclusions and implementation gate

No GET/capture/void endpoints, no PSP simulator execution, no Kafka/Avro message publication, no retry Scheduler/DLQ, no new merchant/security model, no production-scale policies.

**Approved on 2026-10-24:** the 202/400/409 API contract, request validation, SHA-256 canonicalization, no-expiry idempotency for M1, five-record transaction/Outbox contract, and PostgreSQL Testcontainers tests. The sequential split **[M1-01](../tasks/M1-01-transactional-payment-acceptance.md) → [M1-02](../tasks/M1-02-idempotent-replay-and-concurrent-requests.md)** is approved. The user runs Codex locally using the configured orchestration skill; publishing these approved documents does **not** launch Codex, and agents must not invent conflicting new domain rules. Future changes require their own explicit decision and documentation update.
