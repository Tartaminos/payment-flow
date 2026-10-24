# M6-02 — Escalate UNKNOWN After 24 Hours

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Make unresolved PSP operations older than 24 hours visible for required reconciliation and escalation.

## Required Behavior

- Record/reuse `unknown_since` for the original operation; elapsed **24 hours** triggers a durable, inspectable escalation obligation.
- Attempt or schedule permitted safe PSP status resolution and retain the original operation/Payment identities.
- Never turn `UNKNOWN` into `FAILED`/`CANCELED` just because time passed; repeated escalation must not multiply work or charge.

## Scope Lock

**Include:** Durable elapsed-time detection/escalation representation, investigation visibility and simulated-time tests.

**Exclude:** No financial reversal, administrative override, new PSP payment or status change based only on age.

## Authoritative Sources

- [BR5](../requirement/BR-005-reconciliation-after-24h-unknown.md)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)
- [A2](../adr/0002-append-only-audit-and-retention.md)
- [STATES](../domain/payment-state-machines.md)

## Acceptance Criteria

1. An operation at or beyond 24 hours still UNKNOWN is flagged exactly once for reconciliation/escalation, surviving restart.
2. Repeated scheduler/check runs do not create independent financial attempts or duplicate escalations.
3. Operation stays UNKNOWN and Payment retains last confirmed state until authoritative evidence arrives.

## Dependencies

- [M6-01](M6-01-resolve-original-unknown-psp-operations.md)
- M6 contract approval: escalation storage/notification/visibility and status-query scheduling behavior.
