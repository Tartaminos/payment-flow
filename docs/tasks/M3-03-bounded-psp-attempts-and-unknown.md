# M3-03 — Safe PSP Attempts and UNKNOWN

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Apply safe, bounded PSP attempts while preserving ambiguous financial results.

## Required Behavior

- At most three **total** PSP HTTP attempts use request timeouts of 5s, 10s and 20s, and **only** when the fake PSP's idempotency guarantee permits retry under the same stable operation identity.
- Definitive business decline, confirmed non-execution and ambiguous timeout map to `REJECTED`, `FAILED` and `UNKNOWN` respectively; preserve Payment's last confirmed state for UNKNOWN.
- Persist attempt metadata before each external POST and atomically commit UNKNOWN + audit + durable reconciliation job. Deliver an **active independent AUTHORIZE lookup scanner** (not merely a passive row); Kafka redelivery never resets the attempt budget. If the scanner is unavailable or durable handoff is incomplete, withhold offset acknowledgment.

## Scope Lock

**Include:** PSP fault/timeout scenarios, safe retry state transitions, diagnostic persistence and tests.

**Exclude:** No technical Kafka retry backoff (M5), no invented extra PSP attempts, no full reconciliation resolution workflow (M6-01).

## Authoritative Sources

- [M3 proposed implementation contract](../contracts/m3-payment-processor-contract.md) (**PROPOSED — review before implementation**)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [BR6](../requirement/BR-006-distinguish-technical-failure-from-decline.md)
- [BR7](../requirement/BR-007-bounded-psp-retries.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [STATES](../domain/payment-state-machines.md)
- [A10](../adr/0010-generic-execute-payment-operation-command.md)

## Acceptance Criteria

1. Definitive response stops the loop; safe retries never exceed the three approved timeout budgets and preserve the PSP idempotency key.
2. All-ambiguous attempts produce `UNKNOWN` with original Payment state unchanged and traceable evidence; no false DECLINED/FAILED.
3. A PSP response lost **after** its effect is committed is recoverable via the original PSP identity, independent of Kafka replay; crash/redelivery cannot reset `attempt_count` or issue a new independent PSP action. UNKNOWN cannot justify an unsafe offset commit.

## Dependencies

- [M3-01](M3-01-process-simulated-authorization.md)
- M3 contract approval: PSP retry-safe semantics, timeout/error taxonomy and durable UNKNOWN recovery/offset policy.
