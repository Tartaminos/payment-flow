# M6-03 — Traceability and Retention

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Make payment creation, external operation processing and recovery auditable and observable without exposing secrets.

## Required Behavior

- Expose correlated, sanitized diagnostics/metrics across API, Outbox, Kafka, worker, PSP/reconciliation and retry paths; preserve financial audit as an independent PostgreSQL source of truth.
- Implement/document local retention consistent with **5 years** financial/operation/audit history, **30 days** technical logs/traces and **7 days** Kafka records.
- Never purge unresolved financial evidence during routine cleanup; technical log expiry must not prevent business-history reconstruction.

## Scope Lock

**Include:** Correlation propagation, meaningful metrics and safe retention configuration/tests or verifiable local policy.

**Exclude:** No real PAN/CVV/secrets in logs, no assumption that Kafka message retention equals financial audit retention, no production-scale compliance certification.

## Authoritative Sources

- [M6 proposed implementation contract](../contracts/m6-reconciliation-observability-acceptance-contract.md) (**PROPOSED — review before implementation**)
- [NFR1](../requirement/NFR-001-transaction-traceability-and-retention.md)
- [A2](../adr/0002-append-only-audit-and-retention.md)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [A6](../adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)

## Acceptance Criteria

1. Following one payment across API, Outbox, Kafka and PSP yields correlated IDs and meaningful audit/diagnostic evidence.
2. Configured/documented retention boundaries match 5y/30d/7d and preserve durable unresolved evidence; tests verify representative cleanup boundaries.
3. No sensitive payment credentials, secrets or raw provider payloads appear in audit/log traces.

## Dependencies

- [M3-05](M3-05-durable-kafka-offset-progress.md)
- [M4-04](M4-04-guard-expired-authorizations.md)
- [M5-04](M5-04-dispatch-due-technical-retries.md)
- M6 contract approval: correlation convention, metrics/trace collection and realistic local retention enforcement.
