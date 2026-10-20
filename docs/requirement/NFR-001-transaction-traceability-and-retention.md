# NFR-001 — Transaction Traceability and Audit Retention

- **Type:** Non-functional requirement (observability, traceability, data retention)
- **Status:** Accepted — retention periods and audit approach; implementation details open
- **Date:** 2026-10-20
- **Scope:** PaymentFlow's payment lifecycle, PSP operations, audit history, technical logs, and Kafka messages
- **Decision:** [ADR-0002 — Append-only audit trail and retention](../adr/0002-append-only-audit-and-retention.md)
- **Related:** [BR-002](BR-002-uncertain-psp-outcomes.md), [BR-005](BR-005-reconciliation-after-24h-unknown.md), [Domain state machines](../domain/payment-state-machines.md)

## Requirement

> PaymentFlow must provide a durable, chronological and correlatable audit trail of each Payment and PaymentOperation lifecycle, including attempted and confirmed transitions, ambiguous outcomes, reconciliation decisions, and the reason/evidence for each decision.

A current status alone is not sufficient to explain **how** the payment arrived there.

The accepted implementation direction is an **append-only audit/event history in the transactional database**, in addition to the tables storing the current payment/operation state. No full aggregate snapshots are required for each transition in the MVP. This is **not Event Sourcing**.

## Retention periods — accepted project policy

| Data class | Retention | Purpose |
| --- | --- | --- |
| Financial payment/operation history | **5 years** | Investigations, customer disputes and reconciliation |
| Durable financial audit events / status transition history | **5 years** | Reconstruct the sequence of financial and operational decisions |
| Technical application logs and distributed traces | **30 days** | Troubleshooting and operational diagnostics |
| Kafka topic messages | **7 days**, initially | Short-term event replay and consumer recovery |

These are the **agreed engineering defaults for the PaymentFlow case study**, not a universal legal, regulatory, PCI DSS or LGPD retention rule.

The exact retention-clock start (e.g. transaction closure vs. event date), archives, automated deletion/purging mechanisms, and any production regulatory duties will be defined later. Retention must be configurable and documented.

### Exceptions and safeguards

- An unresolved `PaymentOperation.UNKNOWN`, ongoing reconciliation, dispute, or other open recovery process **must not lose the evidence needed to resolve it because a retention deadline elapsed**. Apply a retention hold/extension when necessary, subject to policy and applicable law.
- **Kafka retention is not the financial audit retention policy.** Financial reconstruction and reconciliation must not require a Kafka event that has already expired.
- **Technical logs are not the authoritative financial audit trail.** Deleting a 30-day log must not make the history of a payment unintelligible.
- Purging a Kafka event must not purge the Payment, PaymentOperation, or their durable audit history.

## Required traceability fields (conceptual, not a finalized database schema)

Each durable audit record should contain, where applicable:

- `event_id` — unique event identity.
- `payment_id` — aggregate/payment correlation.
- `payment_operation_id` — specific PSP action, when relevant.
- `event_type` — e.g. `PAYMENT_STATUS_CHANGED`, `OPERATION_OUTCOME_UNKNOWN`, `RECONCILIATION_STARTED`.
- `previous_status` / `new_status` — from the **correct** state machine (Payment or PaymentOperation), when a transition exists.
- `reason_code` and an evidence/reference identifier — why the transition or reconciliation decision happened.
- `occurred_at` and `recorded_at` (UTC) — event time and persistence time; ordering strategy to be defined.
- `correlation_id` / `causation_id` — trace a request or event through the entire chain.
- `source` — API, PSP webhook, worker, reconciliation process, etc.

Persisting *every* intermediate payload is not a requirement. Capture **meaningful domain/operation transitions and decisions** with enough context to investigate; technical traces handle lower-level request details within their shorter lifetime.

## Durability and integrity

- On a status change, save the new current state **and** its corresponding audit event in **one local database transaction**. If either fails, neither is committed.
- The audit history is **append-only at the application level**: corrections are new compensating/superseding records, not silent updates to existing evidence.
- Ensure stable event IDs and prevent duplicate audit entries for reprocessed actions as appropriate; Kafka re-delivery must not trigger duplicate financial changes.
- Access to audit information must be controlled and auditable. The exact permissions, immutability controls, archival strategy, and tamper detection are implementation decisions, not guarantees already implemented.

## Data minimization and security

Use only synthetic payment/card data in this project. Never write full PAN, CVV, API secrets, authentication tokens, or unfiltered sensitive PSP payloads to audit events, logs, or Kafka topics. Store only necessary identifiers, permitted business metadata, and sanitized reason codes.

A five-year audit policy **does not authorize** retaining otherwise unnecessary personal/sensitive data for five years.

## Acceptance criteria

- **AC-001:** Every confirmed Payment/PaymentOperation state transition has a durably persisted and correlatable audit record.
- **AC-002:** State transition and audit event commit atomically in the same database transaction.
- **AC-003:** A timeout leading to `PaymentOperationStatus.UNKNOWN` and its later reconciliation can be traced without overwriting historical events.
- **AC-004:** Business history and audit records follow the configured **5-year** project policy; routine technical logs follow **30 days**, and Kafka messages **7 days**.
- **AC-005:** Financial history remains comprehensible after technical logs and original Kafka messages expire.
- **AC-006:** Active unresolved financial cases are not destroyed by normal data retention housekeeping.
- **AC-007:** Audit entries and technical logs exclude cardholder secrets and unnecessary sensitive PSP data.

## Pending design

- Storage schema/indexes, event ordering, deduplication and schema versioning.
- Retention start date and archival/purge workflows for 5-year records.
- Access controls, tamper-evidence / stronger immutability, backup and disaster recovery.
- Topic-specific retention overrides (if any), dead-letter handling, and consumer replay needs.
- Implementation and operational monitoring of retention holds for unresolved cases.
