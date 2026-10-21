# ADR-0010 — Generic ExecutePaymentOperation Command for Financial Processing

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Logical command contract and handling of payment-operation requests over Kafka
- **Related:** [ADR-0001 — Separate Payment and PaymentOperation](0001-separate-payment-and-payment-operations.md), [ADR-0004 — Synchronous admission and asynchronous PSP execution](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Manual offset acknowledgment](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0006 — Consumer retries versus PSP reconciliation](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [ADR-0007 — Kafka retry topic and DLQ](0007-single-kafka-retry-topic-and-dlq.md), [Conceptual data model](../domain/relational-data-model.md)

## Context

PaymentFlow currently supports three logical PSP operation kinds in the planned MVP: `AUTHORIZE`, `CAPTURE`, and `VOID`. Each is represented by a durable `PaymentOperation` linked to a `Payment`.

[ADR-0004](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md) established that the API **synchronously commits** the Payment/PaymentOperation, idempotency record, audit trail and Outbox intent, while a Payment Processor **asynchronously** executes the externally safe PSP action. The command delivered to Kafka should represent **requested work**, not a claim that the money movement was completed.

Two command shapes were considered: separate `AuthorizePayment`, `CapturePayment` and `VoidPayment` commands, or one generic `ExecutePaymentOperation` referencing the existing operation.

## Decision

**Use one generic command: `ExecutePaymentOperation`, for asynchronous execution of existing logical PaymentOperations.**

- It applies to `AUTHORIZE`, `CAPTURE` and `VOID`. The operation kind is derived from the authoritative persisted `PaymentOperation.type`; if a future message also carries an `operationType`, that field is a consistency hint to validate, **not** an override of database facts.
- The command identifies an **already persisted logical operation**, principally through `operationId`; it does **not** create a fresh financial operation merely because Kafka delivered another copy.
- The same logical PaymentOperation is used across normal Kafka delivery, redelivery, retry-topic routing and scheduler redispatch. Kafka message delivery identities, scheduling work identities, and provider/PSP idempotency identities have distinct purposes.
- A Payment Processor reads and checks authoritative operation/payment state in PostgreSQL, safely acquires/adopts responsibility under the existing concurrency guarantees, validates the intended state transition, and only then considers a provider-safe PSP call.
- The API/Transactional Outbox remains responsible for durably admitting the request **before** publishing the command. This decision does not authorize fire-and-forget publication without a stored PaymentOperation.
- One generic **logical command type** does not imply one physical worker process, one consumer instance or one mandatory Kafka topic. Topic names, partitioning and consumer groups remain separate decisions.

### Illustrative command envelope — initial proposal, subsequently refined

The following example captured the initial discussion; **[ADR-0011](0011-execute-payment-operation-command-envelope.md) subsequently approved the conceptual field responsibilities and current envelope**. This older example is **not** the finalized on-wire format:

```json
{
  "eventId": "uuid-message-001",
  "commandType": "ExecutePaymentOperation",
  "schemaVersion": 1,
  "paymentId": "uuid-payment-123",
  "operationId": "uuid-operation-456",
  "operationType": "AUTHORIZE",
  "correlationId": "uuid-correlation-789",
  "occurredAt": "2026-10-21T18:00:00Z"
}
```

This ADR establishes the **generic command type**, not its transport schema. The **separate [ADR-0011](0011-execute-payment-operation-command-envelope.md)** approves required conceptual fields (`messageId`, `commandType`, `schemaVersion`, `operationId`, `correlationId`, `occurredAt`) and optional `causationId`; `paymentId` in the message body remains undecided and `operationType` is not required. On-wire encoding, Kafka headers and field-format validation are still open; **[ADR-0012](0012-full-compatibility-for-kafka-command-schema-evolution.md)** subsequently approved Full Compatibility as the schema-evolution policy.

Keep the command minimal and free from sensitive cardholder data. The PostgreSQL Payment/PaymentOperation records are the authoritative sources for monetary amount, current status, operation type, stable PSP identity, and PSP HTTP attempt count.

## Commands are not confirmed domain events

| Message concept | Meaning | Guarantees it may communicate |
| --- | --- | --- |
| `ExecutePaymentOperation` **command** | "Process this already admitted financial operation if it is still safe to do so." | **No** claim that PSP execution or financial authorization/capture succeeded. |
| `PaymentAuthorized` **domain event** (example only) | "A payment authorization was definitively confirmed." | Would require an authoritative persisted success before being emitted. |
| `PaymentCaptured` **domain event** (example only) | "A payment capture was definitively confirmed." | Would require an authoritative persisted success before being emitted. |

The architecture already records append-only financial/audit facts in PostgreSQL. This ADR **does not approve publication of `PaymentAuthorized`, `PaymentCaptured` or any new domain-event topic**. Their subscribers, durability contracts and publication needs remain to be justified independently.

## Safety invariants

1. **Duplicate command != duplicate payment.** Idempotent replay identifies the original `PaymentOperation` and never creates a separate PSP operation merely because the command was redelivered.
2. **Database state is authoritative.** A message may be stale or arrive out of order after crossing retry topics. Conflicting, completed, invalid or ambiguous operations must be handled under BR-001, BR-002, BR-004 and BR-008 and prior ADRs.
3. **In-flight PSP ambiguity is preserved.** If the original worker crashed after contacting the PSP, another consumer must not assume the PSP did nothing; resolve/reconcile using the same logical operation and provider identity before any further financial effect.
4. **Operation and messaging retries are distinct.** Kafka retry backoff and scheduling (ADRs 0007–0009) cannot reset the separate, bounded PSP HTTP attempt budget (BR-007).
5. **A command is not an audit fact.** The transactional append-only audit records definitive business/technical changes independently from asynchronous commands.
6. **Offset commits follow durability rules.** A valid command is acknowledged only after the durable outcome or independently recoverable handoff defined in ADR-0005. Unprocessable messages use the DLQ/quarantine path in ADR-0006, not fabricated payment outcomes.

## Alternatives considered

### Separate command type per PSP operation (`AuthorizePayment`, `CapturePayment`, `VoidPayment`)

**Not selected for the MVP.** This gives each action a distinct public message type and may become valuable if payload shapes, ownership or processing requirements diverge significantly; it adds distinct contract surfaces before such differences are necessary.

### Generic `ExecutePaymentOperation`

**Accepted.** Fits the existing PaymentOperation abstraction, minimizes redundant message variants and keeps processing reusable while maintaining type-specific domain validation.

## Consequences and open questions

- Consumers need a single coherent dispatch boundary while retaining **explicit type-specific checks** for authorization, capture and void.
- The command payload can remain compact because financial operation state is already durable in PostgreSQL. This deliberately creates a database read dependency in the processor (consistent with the approved architecture).
- A generic command requires clear validation of unsupported `PaymentOperation.type` values and safe quarantine of malformed/incompatible messages.
- **Accepted subsequently:** the conceptual command envelope, message identities and optional causation were approved in [ADR-0011](0011-execute-payment-operation-command-envelope.md). **Accepted subsequently:** Full Compatibility for schema evolution in [ADR-0012](0012-full-compatibility-for-kafka-command-schema-evolution.md). **Next decision:** serialization technology and contract validation/governance.
- **Other deferred decisions:** finalized topic name, partition key, consumer groups, physical event serialization format, validation rules, replay metadata, authorization/security fields, ownership of result events and any additional domain-event topics.

## Documentation-only boundary

This is documentation of an accepted logical command design. No Java classes, schemas, migrations, Kafka topics/configuration or services are created.
