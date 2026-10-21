# ADR-0007 — Single Kafka Retry Topic and Dead Letter Queue for Consumer Failures

- **Status:** Accepted
- **Date:** 2026-10-21
- **Decision scope:** Kafka consumer retry routing, durable handoff and message quarantine
- **Related:** [ADR-0004 — Asynchronous processing and Transactional Outbox](0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0005 — Manual offset commits](0005-manual-kafka-offset-commit-and-recoverable-processing.md), [ADR-0006 — Consumer retry vs. DLQ vs. financial reconciliation](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md), [BR-002 — Uncertain PSP outcomes](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-007 — Bounded PSP HTTP attempts](../requirement/BR-007-bounded-psp-retries.md), [BR-008 — Conflicting financial operations](../requirement/BR-008-block-conflicting-financial-operations.md)

## Context

An unavailable database or other transient consumer-side processing failure can cause head-of-line blocking: when a Kafka consumer repeatedly attempts the same record, later records on its partition may be unable to make progress.

However, moving a record to a different topic **breaks any cross-topic ordering guarantee**, including for events referring to the same `Payment`. Replaying a Kafka record must never be confused with authorizing another independent PSP financial action.

[ADR-0006](0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md) already distinguishes three cases:

1. **Transient consumer/infrastructure failure** -> recoverable Kafka processing retry.
2. **Definitively unprocessable message** -> durable quarantine/DLQ.
3. **Ambiguous PSP financial result** -> persist `UNKNOWN` and reconcile the **same** PaymentOperation; **not** a Kafka retry/DLQ financial execution loop.

## Decision

For the initial Kafka consumer failure-handling architecture, adopt **one dedicated retry topic and one dedicated DLQ/quarantine topic**.

- **One retry topic:** transiently unprocessable consumer records can be transferred durably so the original partition is not held indefinitely by one failing message.
- **One DLQ:** definitively unprocessable records are transferred to durable quarantine for inspection, correction and controlled replay.
- **No multiple retry topics per delay tier in the MVP.** This may be revisited if real performance or scheduling requirements justify it.
- The **persistent scheduler and logical dispatcher** were subsequently selected in [ADR-0008](0008-persistent-kafka-retry-scheduler.md). The Kafka **consumer retry backoff** was subsequently accepted in [ADR-0009](0009-exponential-backoff-with-jitter-for-kafka-retries.md). Physical scheduler implementation, operational escalation thresholds and detailed failure classification still require design. A Kafka topic by itself does **not** schedule delayed delivery.
- This decision concerns **Kafka message-processing retries only**. The independent PSP HTTP retry limit (three total attempts with request timeouts **5s, 10s, 20s**, only when provider-safe) remains unchanged.

### Proposed topic names — not yet finalized contracts

| Role | Candidate name | Decision status |
| --- | --- | --- |
| Main financial operation requests | `payment.operations.requested.v1` | Previously proposed; exact topology/naming not yet approved |
| Single Kafka consumer retry destination | `payment.operations.retry.v1` | **One dedicated retry topic approved**; name is proposed |
| Single DLQ/quarantine destination | `payment.operations.dlq.v1` | **One dedicated DLQ approved**; name is proposed |

These names are illustrative. Partition count, consumer group identity and exact message formats remain open. **[ADR-0018](0018-payment-id-as-kafka-command-partition-key.md) subsequently selects `paymentId` as the Kafka key for financial `ExecutePaymentOperation` command publications**, without finalizing the retry wrapper format or any DLQ key.

## Conceptual routing

```text
Payment operation message (main topic, or retry topic)
                   |
                   v
          Payment Processor
                   |
         Classify observed outcome
            /       |       \
           /        |        \
    Processed     Transient   Definitively unprocessable
       |             |                   |
       v             v                   v
 Durable result   Durable handoff     Durable handoff
 or recoverable   to one retry topic  to one DLQ topic
 handoff              |                    |
       |              |                    |
       v              v                    v
 Manually commit source offset only after the corresponding
 durable processing outcome / handoff is confirmed
```

An **uncertain PSP result** takes a different path: persist the original operation as `UNKNOWN` and an independently durable reconciliation responsibility before acknowledging Kafka consumption. It is **not** sent to the consumer retry topic to reissue the financial action, and **not** treated as a poison message.

## Durable forwarding and offset acknowledgment

1. Preserve a stable original `eventId`, `paymentId`/`operationId` where available, `correlationId`, source topic/partition/offset, failure classification, retry diagnostic count and relevant timing metadata in the retry/quarantine context. Exact envelope and data protection rules are deferred.
2. **Confirm the retry-topic or DLQ handoff is durable before committing the source offset.** A broker acknowledgment consistent with the future durability contract, or another independently durable recovery record, must exist. A merely attempted or buffered publish is insufficient.
3. Transferring a message and committing the source Kafka offset are **not one atomic action** under this design. If the handoff succeeds but the consumer crashes before committing the source offset, the message may appear in both places. Downstream consumers therefore must tolerate duplicates by the original event/operation identity.
4. For each source partition, commit only contiguous offsets whose records have durably completed or durably handed off; never skip a preceding incomplete record.
5. A record taken from the **retry topic** follows the **same** financial identity, admission checks, failure classification and durable-handoff rules. Avoid a hot loop between the retry topic and itself: the precise scheduling mechanism and retry budget must be chosen before implementation.
6. Transferring a business-message retry to another topic is not itself completion of the underlying `PaymentOperation`. PaymentFlow must retain the original operation and ensure its recovery is tracked; a Kafka retry count is **not** a PSP HTTP attempt count.
7. For DLQ/quarantine, preserve enough safely redacted information to diagnose and later reprocess the **same logical operation** if it becomes possible. Quarantine is not a financial rejection or failure.

## Ordering and state safety

Moving records between topics removes any guarantee that Kafka will deliver related operations to the Payment Processor in their original relative order. For example:

```text
Authorization request for Payment 123 -> retry topic (database unavailable)
Capture request for Payment 123       -> main topic (arrives later, processes first)
```

Even if the authorization request is delayed, the capture **must not** be sent to the PSP until the underlying Payment is authoritatively `AUTHORIZED` and all other domain admission conditions hold.

If such a capture request is valid but **not yet eligible**, PaymentFlow must **not discard it after a simple status check**. It must be rejected according to a defined API contract or safely retained/deferred through a durable workflow, as appropriate; that exact behavior is a separate design decision.

The database/domain model, not cross-topic Kafka ordering, enforces authorization/capture/void invariants and the prohibition on new conflicting financial operations (BR-008). Kafka retry routing does **not** grant permission to reset an `UNKNOWN` or `PROCESSING` operation or to create another independent PSP charge.

## Alternatives considered

### A. Retry only in the original consumer partition

**Not selected as primary strategy.** Simple and retains local ordering, but sustained infrastructure errors may block later unrelated records in the same partition.

### B. Multiple Kafka retry topics grouped by delay duration

**Deferred for later evaluation.** May simplify some operational timing policies, but multiplies topics, routing and consumer orchestration before throughput/backoff requirements are defined. ADR-0008 subsequently selects durable scheduling behind a single retry topic.

### C. Single Kafka retry topic plus single DLQ

**Accepted for the MVP.** Provides a simpler error-routing topology while leaving room for controlled scheduling and explicit, durable quarantine.

## Consequences and limitations

- **Benefit:** failing individual records need not monopolize a source partition indefinitely if durable handoff works.
- **Benefit:** separation of transient infrastructure problems from permanently unprocessable messages and financial reconciliation.
- **Trade-off:** producer/consumer handoff, delayed-eligibility scheduling, duplicate detection, replay and monitoring introduce additional operational work.
- **Trade-off:** topic-crossing can reorder operations for the same payment; all financial state transitions require authoritative database-level checks and safe handling of temporarily ineligible operations.
- **Safety boundary:** a successful Kafka handoff plus source offset commit protects *message processing progress*, not end-to-end exactly-once financial execution.

## Explicitly open for future design

- The Kafka consumer **backoff schedule** (10-second initial delay, 2× growth, ±20% jitter and hard five-minute cap) and **absence of automatic DLQ on fixed transient retry count** were accepted in [ADR-0009](0009-exponential-backoff-with-jitter-for-kafka-retries.md). Operational thresholds, pressure limits and incident escalation remain open, separately from PSP request timeouts.
- A **persistent scheduler and due-work dispatcher** were approved in [ADR-0008](0008-persistent-kafka-retry-scheduler.md) for delayed retries without keeping consumer partitions paused. Exact persistence schema, selection/leases, dispatcher reliability and timed eligibility remain open.
- **[ADR-0018](0018-payment-id-as-kafka-command-partition-key.md) chooses `paymentId` for financial command publications**. **[ADR-0019](0019-canonical-utf8-uuid-kafka-command-key.md) chooses canonical lowercase UTF-8 UUID encoding for the `ExecutePaymentOperation` Kafka key**. Final topic names, partition counts, consumer groups and the retry-specific envelope/key remain to be designed.
- Reliable publishing/acknowledgment strategy and any transactional/Outbox-like handoff for **consumer-produced retry messages**.
- Dead-letter retention, permission model, diagnostics, alerting, operator runbook and safe redrive mechanism.
- Durable workflow for valid-but-not-yet-eligible operations under possible cross-topic reordering.
- Crash/rebalance recovery while a PSP request is in flight, coordination with manual offsets, leasing and reconciliation.

## Documentation-only boundary

This ADR is an architectural decision record only. No Kafka topic, configuration, Java class, migration, executable SQL or runtime service is created.
