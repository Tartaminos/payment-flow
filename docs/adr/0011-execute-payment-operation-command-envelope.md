# ADR-0011 — ExecutePaymentOperation Command Envelope and Message Identity

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Logical command envelope, correlation and identity under Kafka delivery/retry
- **Related:** [ADR-0004 — Synchronous admission, Outbox and async PSP processing](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Durable-before-commit Kafka offsets](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0007 — Retry topic and DLQ](0007-single-kafka-retry-topic-and-dlq.md), [ADR-0008 — Persistent Retry Scheduler](0008-persistent-kafka-retry-scheduler.md), [ADR-0010 — Generic ExecutePaymentOperation command](0010-generic-execute-payment-operation-command.md), [Conceptual PaymentOperation model](../domain/relational-data-model.md)

## Context

ADR-0010 approved one logical command, `ExecutePaymentOperation`, for previously admitted `AUTHORIZE`, `CAPTURE` and `VOID` PaymentOperations. We need sufficient metadata to identify each command publication, correlate its multi-step processing/retries and support future contract evolution without treating a Kafka message as a financial fact.

Kafka **redelivery** of a retained record and **republication** of a new retry command are distinct scenarios. PaymentFlow must keep the financial identity stable even when a later publication has a different message identity.

## Accepted envelope

The conceptual `ExecutePaymentOperation` command carries the following attributes:

| Field | Presence | Agreed meaning |
| --- | --- | --- |
| `messageId` | Required | Stable identity of **one logical message publication**. Remains the same when that same Kafka record is delivered again. A **new** republished command (e.g., via the Retry Dispatcher) may have a **new** `messageId`. |
| `commandType` | Required | Command discriminator; currently literal `ExecutePaymentOperation`. |
| `schemaVersion` | Required | Version identifier used to interpret the command contract. The illustrative initial value is `1`; precise compatibility/evolution rules are still deferred. |
| `operationId` | Required | Stable identity of the **existing durable PaymentOperation**; unchanged across Kafka redelivery, retries and new command publications. |
| `correlationId` | Required | Identifier connecting the original request and downstream workflow, including retries and diagnostics. Normally preserved through this processing chain. |
| `occurredAt` | Required | Time at which **this command publication was created**, **not** when the PSP executed/authorized/captured the payment. |
| `causationId` | Optional | Prior message/causal trigger that led to this publication; useful, for example, for relating a newly dispatched retry to a previous message. Exact causation graph semantics remain open. |
| `paymentId` | **Undecided** | May be included in the body, but not required by this ADR: `operationId` already identifies a PaymentOperation and its Payment in the database. Also a candidate for a future Kafka partition key. |

`operationType` is **not required** in the command envelope; the authoritative `PaymentOperation.type` (`AUTHORIZE`, `CAPTURE`, `VOID`) is looked up from the database, per ADR-0010. No PSP credential, cardholder detail, sensitive provider payload, financial amount, or authoritative business state should be inferred from the message.

### Conceptual initial publication

```json
{
  "messageId": "c13a7544-1f79-45f4-9312-8993319e7a1d",
  "commandType": "ExecutePaymentOperation",
  "schemaVersion": 1,
  "operationId": "b8d7106f-5930-4b6b-b7fb-08985070a91d",
  "correlationId": "91d0d856-3e2a-4df5-bba6-c92797d26db6",
  "occurredAt": "2026-10-21T18:00:00Z"
}
```

### Conceptual command after a persistent retry

```json
{
  "messageId": "9b8dd6c9-1044-4cc8-a2c3-ea96876423cb",
  "commandType": "ExecutePaymentOperation",
  "schemaVersion": 1,
  "operationId": "b8d7106f-5930-4b6b-b7fb-08985070a91d",
  "correlationId": "91d0d856-3e2a-4df5-bba6-c92797d26db6",
  "occurredAt": "2026-10-21T18:00:14Z",
  "causationId": "c13a7544-1f79-45f4-9312-8993319e7a1d"
}
```

These examples illustrate agreed field roles. The exact wire schema/serialization, validation grammar, placement in Kafka headers versus body, timestamp formats, uniqueness constraints and whether `paymentId` will appear in the payload will be decided in subsequent contract design. No topic names or partition keys are finalized here.

## Identity semantics and replay

1. **Same Kafka record redelivered:** `messageId`, `operationId` and `correlationId` remain unchanged; the broker may deliver the same record again after a consumer crash or offset failure.
2. **New, intentionally republished command:** the new logical publication may use a **new `messageId`**, while maintaining `operationId` and the flow's `correlationId`; `causationId` may identify the publication that led to it.
3. **The Outbox producer retried sending the same logical publication** after an uncertain Kafka acknowledgment: it may resend the **same `messageId`** even though Kafka stores two physical records. Do not define `messageId` as automatically regenerated on every producer transmission attempt.
4. **`operationId` is the financial idempotency boundary in this command flow**, not `messageId`. Two distinct message IDs can still address the same financial operation; the consumer must inspect existing PaymentOperation state, database concurrency guarantees and stable PSP identity before any external financial effect.
5. **`correlationId` is for traceability, not financial uniqueness**. A single correlation flow can span multiple events/commands and operations.
6. **`causationId` is optional**, so consumers cannot demand it to process otherwise valid commands. Original requests will often have no predecessor command; more precise provenance rules can be introduced later.
7. **Scheduled retry identity is separate from message identity.** A crash after dispatch may produce duplicate Kafka records or even new publications for the same scheduled work; the future Retry Scheduler persistence contract must prevent accidentally multiplying work or resetting the PSP budget.

## Validation and safe handling

- The command is a **request to process**, not a confirmed domain event such as `PaymentAuthorized` or `PaymentCaptured`. Emitting financial domain events remains a separate, unapproved decision.
- The worker consults the persisted `PaymentOperation` by `operationId` and validates its `Payment`, type, status and concurrency/idempotency conditions before considering a PSP call; a Kafka envelope never overrides the database.
- If the operation is already completed, an idempotent no-op/acknowledgment is permitted under ADR-0005. If PSP execution may already have occurred but the result is ambiguous, use `UNKNOWN` plus reconciliation rather than blindly charging again.
- Unsupported/incompatible or definitively invalid commands follow the durable DLQ/quarantine policy of ADR-0006; a schema mismatch is not a payment decline.
- A correctly formed command arriving before a temporarily unavailable database can be retried under ADRs 0007–0009; message processing retries do not reset the three-attempt PSP HTTP budget.

## Alternatives and rationale

### Identifying the command solely with `messageId`

**Rejected.** Retries may republish under new message IDs even though they relate to the **same** PaymentOperation, so message uniqueness cannot ensure financial idempotency.

### Reusing `operationId` as both message ID and financial ID

**Not selected.** Prevents distinguishing multiple publications and loses useful messaging diagnostics and causality.

### Separate message publication, operation and correlation identifiers

**Accepted.** Supports at-least-once delivery, meaningful event tracing and financial consistency anchored in durable PaymentOperation identity.

## Not decided here

- Precise schema-evolution rules (forward/backward compatibility, additive versus breaking changes, supported schema versions, unknown fields and schema registry approach).
- Whether this JSON example becomes the final on-wire encoding; field-format specifications, payload/headers placement, schema governance and validation errors.
- Whether `paymentId` appears in payload or only in the **future** Kafka record key; exact partition key/partitioner/consumer-group/topic names and counts.
- Outbox publisher's practical identity-generation/lifecycle, Retry Scheduler deduplication key, dispatcher publication identity and concurrency.
- Domain event publication (`PaymentAuthorized`/`PaymentCaptured`), PSP webhooks or future notification contracts.

## Documentation-only boundary

This ADR records an approved **conceptual command-envelope contract**. It creates no implementation code, Kafka schema registration, runtime topic, SQL migration or running service.
