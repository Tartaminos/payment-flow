# ADR-0002 — Append-only audit trail and retention policy

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decision scope:** Transaction traceability, event history, and retention
- **Requirement:** [NFR-001 — Transaction Traceability and Audit Retention](../requirement/NFR-001-transaction-traceability-and-retention.md)

## Context

[ADR-0001](0001-separate-payment-and-payment-operations.md) separated the current business state of a `Payment` from the execution state of each `PaymentOperation`.

A current status cannot explain all the significant intermediate actions and evidence: an authorization request, PSP timeout, later webhook, reconciliation investigation, capture result, or a rejected duplicate request.

We need an inspectable, durable timeline and an explicit distinction between financial evidence, application logs and retained Kafka messages.

## Decision

1. Keep the **current authoritative state** in the transactional Payment and PaymentOperation records (PostgreSQL is the candidate database).
2. Persist an **append-only, domain-oriented audit history** of meaningful state transitions and decisions, referencing Payment/PaymentOperation IDs, transition status, evidence, timestamps and correlation/causation metadata.
3. Write audit records and the corresponding current-state mutation **atomically in the same local database transaction**.
4. Do **not** introduce full state snapshots at every step or Event Sourcing for the MVP.
5. Accept the following initial **project retention periods**:
   - **5 years** for financial transaction/operation history.
   - **5 years** for durable audit records.
   - **30 days** for technical logs/traces.
   - **7 days** for Kafka messages.
6. Treat Kafka as an asynchronous event log/transport for short-term replay, **not** the only source of durable financial evidence.
7. Apply retention safeguards to unresolved operations, reconciliation cases and disputes; do not delete required evidence while a financial outcome remains unresolved. Retain only the minimum necessary data and exclude secrets/cardholder data.

The policy's precise retention start event, archiving mechanism, storage schema, authorization model, and deployment controls are deliberately deferred. The retention values represent a **case-study engineering choice**, not a claim of legal/regulatory compliance.

## Alternatives considered

### A. Store only the latest Payment/PaymentOperation status

**Rejected.** Loses information required to understand timeouts, retries, provider responses and reconciliations.

### B. Store a full snapshot of the entire aggregate on each step

**Not adopted for MVP.** Repeats large amounts of state and sensitive data with little benefit for reconstructing the relatively small number of meaningful domain transitions. Snapshots may be evaluated later for performance or operational reasons.

### C. Event Sourcing with Kafka as the source of truth

**Rejected for MVP.** Introduces replay and domain rebuilding complexities that are not required to satisfy the current scope. Kafka's **7-day** retention is also intentionally much shorter than the **5-year** audit history.

### D. Append-only audit events in the transactional database

**Accepted.** Clear transaction boundary with the state being changed, durable long-term history, and no reliance on Kafka retention for investigations.

## Consequences and trade-offs

**Benefits**
- Traceable payment histories, including `UNKNOWN` and recovery.
- More reliable audit evidence because state and audit records share one commit.
- Reduced coupling between debugging log retention, Kafka message retention and financial history.
- Smaller and safer records than blanket payload snapshots.

**Costs / limitations**
- Additional write/storage cost, retention lifecycle, and query/indexing work.
- Database-level immutability, archival, evidence integrity, and disaster-recovery measures must still be designed; “append-only” in the application is not proof against privileged database modification.
- Audit events are **not automatically integration events**; Kafka delivery may need an Outbox and separate publication tracking.
- Correlation IDs improve tracing but do not replace financial identity/idempotency keys.
- Long retention must be reviewed against applicable security/privacy/legal duties for any actual production system.

## Related design tasks

- Define audit event contracts and database schema.
- Decide current-state vs. audit-update transactions and eventual Outbox boundaries.
- Design access controls, safe PSP metadata, retention enforcement and preservation of unresolved operations.
- Determine how to handle correction events, idempotent delivery, and lifecycle events that do not change a business status.
