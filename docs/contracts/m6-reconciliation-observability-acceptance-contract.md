# M6 — Reconciliation, Escalation, Retention & End-to-End Acceptance Contract

- **Status:** **PROPOSED — pending explicit user approval** (2026-10-24). All M6 task files remain PLANNED.
- **Authority:** [BR-002](../requirement/BR-002-uncertain-psp-outcomes.md), [BR-005](../requirement/BR-005-reconciliation-after-24h-unknown.md), [BR-006](../requirement/BR-006-distinguish-technical-failure-from-decline.md), [NFR-001](../requirement/NFR-001-transaction-traceability-and-retention.md), [ADR-0002](../adr/0002-append-only-audit-and-retention.md), [ADR-0005](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md), and the [M3](m3-payment-processor-contract.md)/[M4](m4-capture-void-expiration-contract.md)/[M5](m5-technical-retry-dlq-scheduler-contract.md) proposals.
- **Principle:** Reconciliation is evidence-based **status lookup of the original external operation**, not a new financial charge or an excuse to mark UNKNOWN as FAILED.

## M6-01 — Generalize active reconciliation

1. **Do not wait until M6 to create recovery:** M3 creates the durable `reconciliation_jobs` table, active scanner and safe lookup for AUTHORIZE; M4 extends safe lookup/independent recoverability to CAPTURE and VOID before acknowledging their UNKNOWN offsets. **M6-01 completes and harmonizes** authoritative response mapping, conflict handling, late results, retried lookups, audit and transitions for **all three** operation types. Reuse the same job table, locks, identity and scheduler; no second reconciliation pipeline.
2. Provider identity = original `PaymentOperation.id` UUID / stable PSP idempotency key. Lookup `GET /simulator/operations/{pspOperationId}`, never POST to discover status. Confirmed `SUCCEEDED/REJECTED/FAILED-nonexecution` maps to permitted operation/payment state transition; uncertain/not found/timeout/5xx continues UNKNOWN. `404` by itself is **not** proof of no effect.
3. Each evidence item records provider decision/reference, evidence timestamp and correlation; apply DB conditional status check to avoid stale evidence overwriting a later confirmed outcome. Append one audit event per actual transition in the same transaction. Duplicate/late result → no duplicate financial effect or audit transition.
4. Poll due jobs every **5s** in batches of **20**; HTTP lookup timeout **3s**, per-job DB lease **30s** with owner fencing and retry delay `min(300s, 5s × 2^attempts)` for ongoing ambiguity (no automatic failure). Workers limit to **4 simultaneous lookup requests**. Preserve `unknown_since` from the *first* UNKNOWN entry. Store last lookup/evidence/result and current next check durably; do not reset on process restart.
5. On irreconcilable contradictory evidence, retain UNKNOWN (or prior confirmed final state), create a visible escalation and **never force a state transition**. PSP provider evidence, not Kafka offset/message, is the financial source of truth.

## M6-02 — 24-hour escalation

- Threshold begins at **first entry into UNKNOWN (`unknown_since`)**, not latest PSP contact or Kafka dispatch. At **`unknown_since + 24 hours` inclusive**, if still UNKNOWN, create a **single unique durable escalation** in `reconciliation_escalations` with `operation_id` unique, `created_at`, `reason=PSP_UNKNOWN_24H`, `status=OPEN`, `last_notification_at` nullable, and last evidence reference.
- Escalation processing uses the same scanner/PSP lookup workflow, with investigation information accessible through **database-backed operational views and metrics/logs** in the MVP; no new unauthenticated external administrative HTTP endpoint.
- On authoritative resolution, close the escalation with evidence and append one audit record; no invented FAILED/CANCELED due solely to age. No duplicate alerts or escalation rows across repeated scans/restarts; escalate conflicting evidence immediately as a separate reason using the same unique operation obligation.
- Tests with injectable clock at 23:59:59 and 24:00:00, duplicate scheduler calls, provider down, restart, and late confirmed result.

## M6-03 — Audit, observability, data retention

- Use a propagated `correlationId` across POST API, persisted Outbox, Kafka Avro, worker and PSP calls, retry/scheduler and reconciliation. Include safe `paymentId`, `operationId`, `messageId`, `retryId` fields when available in **structured JSON application logs**. Propagate distributed trace context via optional Kafka/HTTP technical headers when available, but **never require headers for financial identity**.
- PostgreSQL `payment_audit_events` is append-only; every transition persists `event_id`, `payment_id`, operation id when present, previous/new correct status, reason/evidence reference, `occurred_at`/`recorded_at` in UTC, `correlation_id`, `source` and per-payment sequence. No raw PAN/CVV, secret, token, authorization headers, PSP payload dumps or cardholder data in logs/audit.
- Spring Boot Actuator/Micrometer Prometheus local endpoints and metrics: counters of API admissions, Outbox pending/oldest, commands consumed, PSP definitive/UNKNOWN, duplicate/replay, reconciliation due/escalated, technical retry backlog/oldest and DLQ entries; timers for PSP lookup/financial call and publication. Metrics labels use **bounded enums**, **not** IDs, orderIds or raw URLs to avoid high cardinality. Bind diagnostic endpoints locally in the educational setup, not publicly exposed by default.
- **Financial Payment/PaymentOperation and audit: minimum 5 years from recorded_at UTC.** A monthly cleanup candidate may purge only records older than that threshold **when explicitly enabled** and must respect `UNKNOWN`, OPEN reconciliation/escalation, active retry, dispute/retention holds and FK/audit integrity. For local education **dry-run/no destructive purge by default**; test-mode verifies actual guarded deletion is possible when explicitly enabled.
- **Technical logs/traces: 30 days** via bounded rotated files or an appropriately configured local exporter/storage with 30-day retention; they are diagnostics, not the financial source of truth. **Kafka topics: 7-day retention**, main, retry and DLQ by default. A long-running retry/DLQ case must have **independent durable PostgreSQL evidence**; expiry of Kafka records cannot erase an unresolved case.
- **Idempotency key rows from M1/M4 are not automatically purged** under these retention choices. Outbox and retry/quarantine/reconciliation supporting rows can be archived/cleaned only after durable completion, required retention and no unresolved recovery hold. No claim of production PCI, legal/regulatory compliance or immutable object storage in this educational MVP.
- Demonstrate 5-year/30-day/7-day boundaries and retention holds with deterministic UTC test clock and fault injection; deleting a 30-day log or 7-day Kafka record must still allow reconstruction via PostgreSQL audit.

## M6-04 — Reproducible distributed-failure acceptance

- Provide one reproducible automated integration suite using **real PostgreSQL, Kafka and Schema Registry** plus the stateful simulated PSP (locally via Docker/Testcontainers or controlled services), with deterministic clock/random/failure toggles; never real card data.
- Required independent fault cases:
  1. Client response dropped after DB commit → same `Idempotency-Key` returns original Payment and only one operation/outbox.
  2. Kafka publish ack received but Outbox DB update lost → at-least-once duplicate has same original `messageId`; no second PSP effect.
  3. Broker/Registry outage during publication → pending Outbox recovers later.
  4. Command redelivery/out-of-order worker completions and rebalance → contiguous offset advancement only after durable eligibility.
  5. Fake PSP persists authorization result then HTTP response disappears → Operation UNKNOWN/recovery resolves original provider identity, no new independent operation.
  6. Concurrent capture vs capture/void/expiry, and PSP definite expiration vs unknown validity → no double charge/capture or falsely canceled authorization.
  7. Technical retry handoff + scheduler DB failure/restart + duplicate dispatcher publication → no skipped command or reset PSP attempt budget.
  8. Poison Avro record → durable DLQ/quarantine before offset progress and recoverable original schema identity.
  9. UNKNOWN still open at 24h → exactly one escalation without fake FAILED status; confirmed late resolution recorded once.
  10. Financial audit remains complete when test-expired Kafka records/logs are removed; no sensitive data leaks.
- Each case asserts **Payment state, operation state/identity, audit count, Outbox/retry obligation, Kafka offset eligibility** where applicable; no requirement that all network failures resolve within a real-time SLA. Work that cannot yet be resolved must remain observably UNKNOWN and independently recoverable.
- Run integration verification via separate Maven profile/tag in CI/local to avoid forcing external containers for every unit test. Include setup/teardown commands and documented environment prerequisites.

## Explicit exclusions and gate

No live PSP, PCI certification, production HA/multi-region, automatic refunds, arbitrary admin mutation of Payment status, revenue ledger or externally exposed audit dashboard. M6 is the **verification/completion milestone** of the planned educational system, not a production compliance certification.

**Sequence:** [M6-01](../tasks/M6-01-resolve-original-unknown-psp-operations.md) → [M6-02](../tasks/M6-02-escalate-unknown-after-24h.md) / [M6-03](../tasks/M6-03-traceability-and-retention.md) → [M6-04](../tasks/M6-04-end-to-end-failure-recovery.md). All remain PLANNED until explicit user acceptance of this contract and its upstream contracts.
