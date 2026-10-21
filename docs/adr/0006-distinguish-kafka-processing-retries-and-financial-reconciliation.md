# ADR-0006 — Separate Kafka Processing Retries, Poison-Message Quarantine and Financial Reconciliation

- **Status:** Accepted
- **Date:** 2026-10-21
- **Scope:** Kafka consumer processing failures and recovery boundaries for PaymentFlow
- **Related:** [ADR-0004 — Transactional Outbox and asynchronous processing](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Manual offset commits](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [BR-002 — Uncertain PSP outcomes](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-005 — Reconciliation](../requirement/BR-005-reconciliation-after-24h-unknown.md), [BR-007 — Bounded PSP attempts](../requirement/BR-007-bounded-psp-retries.md), [BR-008 — Block conflicting financial operations](../requirement/BR-008-block-conflicting-financial-operations.md)

## Context

Kafka may deliver the same message more than once and a Payment Processor may fail for several unrelated reasons: an unavailable PostgreSQL database, an invalid/unsupported event, or a PSP request whose financial result cannot be confirmed.

Treating all of these cases as one generic retry loop is unsafe. **Reprocessing a Kafka message is not authorization to issue a new PSP financial request.** Retrying a PSP call is a different operation with a separately bounded, contract-dependent policy (BR-007).

The architecture must distinguish transport/processing recoverability from **financial outcome certainty**.

## Decision

Adopt three separate handling paths:

| Failure category | Illustrative case | Accepted handling | Financial implication |
| --- | --- | --- | --- |
| **Transient Kafka-consumer/infrastructure failure** | PostgreSQL temporarily unavailable before the consumer can safely handle an event | Controlled consumer-side retry with backoff; retain a recoverable processing path. Do not commit a Kafka offset before an outcome or recovery handoff is durable under ADR-0005. | Does **not**, on its own, mean a PSP operation failed, and does not authorize another financial action. |
| **Definitively unprocessable Kafka message (poison message)** | Malformed message or unsupported schema/operation type that cannot be processed normally | After classification, route to a **durable quarantine / Dead Letter Queue (DLQ)** for diagnosis and safe correction/reprocessing. Offset may advance **only after** the quarantine handoff is acknowledged as durably complete and the record can be found/recovered. | Quarantine **does not** mean `Payment.status = DECLINED` or `PaymentOperation.status = FAILED`. Preserve the status of any underlying logical financial operation. |
| **Uncertain external PSP financial result** | Timeout, lost response, or ambiguous outcome after a capture request | Persist the original `PaymentOperation` as `UNKNOWN`, retain the Payment's last confirmed state, and durably schedule/record independent **reconciliation**. Commit the Kafka offset only after that handoff is durable. | **Not a poison message**. Do not automatically send the financial operation to DLQ or create a second independent charge. |

**DLQ/quarantine is limited to messages that are definitively not processable through the normal consumer flow**, not a default sink for infrastructure downtime or PSP timeouts. The precise conditions under which a temporarily failing message might later be classified as definitively unprocessable are left for policy design; exhausting an arbitrary retry count alone is not proof of permanent failure or safe financial non-execution.

## Boundaries and safety invariants

1. **Kafka processing retry ≠ PSP financial retry.** A record redelivered many times does not reset or extend the approved **three total PSP HTTP attempts**, with per-request timeouts of **5s, 10s, 20s**, under the same logical PaymentOperation and stable external idempotency identity.
2. **Never derive payment failure from Kafka delivery failure.** A rejected authorization is `AUTHORIZE: REJECTED` with `Payment: DECLINED`; a proven technical non-execution is `PaymentOperation: FAILED`; an ambiguous PSP outcome is `UNKNOWN` (ADR-0003).
3. **Protect the underlying operation before acknowledging quarantine.** For a poison message, preserve original event identity, correlation information, failure classification and, whenever available, Payment/PaymentOperation reference. If an event cannot be decoded or correlated, quarantine it with sufficient original safe metadata for investigation without inventing a financial outcome.
4. **Quarantine must be durably confirmed before committing the originating offset.** Simply attempting to publish to the DLQ is not a reliable handoff. If the DLQ publish or its durable tracking fails, do not skip the source record.
5. **Quarantined messages may require operational investigation** and must support controlled replay/recovery after correction. A replay must always reuse existing logical operation identity and pass idempotency/concurrency checks; it must not unconditionally create a new PSP request.
6. **Preserve the Payment's last confirmed business state** when a PSP result is unknown; recovery is carried by the original operation and the reconciliation workflow, not Kafka replay as a new payment.
7. **Temporary infrastructure failures must not be silently swallowed.** Keep retrying through a controlled policy, or move responsibility to an explicitly durable recovery path before advancing the offset. Avoid infinite busy loops.
8. **Partition head-of-line blocking is a real trade-off.** A single repeatedly failing record can delay subsequent records in that partition. Retry topics, quarantine, and parallelism may change effective per-payment ordering; PaymentFlow cannot rely on topic/partition order to substitute for transactional business validation.
9. **No automatic new independent financial operation while an earlier conflicting operation is unresolved**, as specified in BR-008.

## Failure classification matrix

| Observed condition | Likely classification | Actions permitted | Actions prohibited |
| --- | --- | --- | --- |
| PostgreSQL connection refused before operation reservation | Transient consumer/infrastructure failure | Consumer retry after controlled delay; pause/resume processing as needed | Commit offset with only an in-memory intention; call PSP without durable admission |
| Event payload violates the agreed schema and cannot be parsed | Definitively unprocessable Kafka message | Durable quarantine, diagnostic metadata, alert/investigation, controlled replay after correction | Blindly create `PaymentOperation: FAILED` or a PSP action |
| Unsupported event version or operation type | Definitively unprocessable under current consumer contract, pending investigation | Durable quarantine with version/type context | Unbounded hot-loop retries without intervention |
| Duplicate event for already finalized operation | Valid duplicate | Idempotently recognize and acknowledge under ADR-0005 | Dispatch a second independent financial effect |
| PSP HTTP timeout after an attempted capture | Financially uncertain PSP outcome | Same-operation safe status check; `UNKNOWN`; durable reconciliation; eventual offset acknowledgment after handoff | Generic DLQ classification, mark `FAILED` from timeout, or create new independent capture |
| Worker crashes after PSP execution but before database result commit | Potentially ambiguous financial outcome | Recover original logical operation; verify PSP reference/status; reconcile safely | Treat Kafka redelivery as proof the PSP was never contacted |

## Alternatives considered

### A. Apply a single retry policy to every failure, followed by DLQ

**Rejected.** Mixes consumer reliability problems with external financial uncertainty, risking repeated external charges or false financial failures.

### B. Send uncertain PSP outcomes to DLQ

**Rejected.** The Kafka record may be valid even though the external financial operation has an unknown result; the correct recovery is financial reconciliation tied to the existing operation.

### C. Separate controlled consumer retries, permanent-message quarantine and financial reconciliation

**Accepted.** Gives each class of failure an explicit recovery path with consistent offset and idempotency guarantees.

## Consequences and trade-offs

- Clearer operational accountability: DLQ alerts refer to message-processing problems, while `UNKNOWN`/reconciliation refers to financial uncertainty.
- Requires a durable quarantine handoff and an investigation/replay process before an offset can safely advance past a poison message.
- Temporary infrastructure outages may still block a partition until recovered or durably transferred; avoiding head-of-line blocking requires further design.
- Duplicate Kafka delivery and a DLQ redrive cannot bypass BR-001, BR-004 or BR-008.
- More state and operational complexity than indiscriminate retries, but avoids conflating different failure classes.

## Explicitly deferred decisions

- Exact Kafka retry backoff, retry budget, timing, and strategy: **local delayed retries versus dedicated retry topics**.
- DLQ/quarantine topic name, payload envelope, required redaction, retention, access controls, alerting/ownership and redrive process.
- Whether a PostgreSQL quarantine/inbox record is needed in addition to, or instead of, Kafka DLQ for durable acknowledgment.
- Concrete offset/acknowledgment coordination with DLQ publication and consumer-group rebalances.
- Head-of-line blocking mitigation and per-payment ordering guarantees under retry topics, delayed processing or out-of-order completion.
- Topology decisions (topic count, stable partition key, group, partition count) remain **unapproved proposals** as of this ADR.
- PSP status-query and reconciliation scheduling mechanism; worker recovery/lease semantics and the exact point of `UNKNOWN` transition.

## Documentation-only boundary

This ADR records agreed architecture semantics. It does **not** create Java code, SQL, migrations, Kafka topics/configuration, applications or infrastructure.
