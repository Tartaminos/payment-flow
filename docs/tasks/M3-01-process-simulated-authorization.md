# M3-01 — Consume and Resolve Authorization

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Process an existing AUTHORIZE command with a simulated PSP and record verified financial outcomes.

## Required Behavior

- Decode the command and load the already persisted `PaymentOperation`; never manufacture an operation from Kafka delivery.
- Call the fake PSP outside the original API request and outside long-held database transactions; confirmed approve yields `AUTHORIZED`/`SUCCEEDED`, confirmed decline yields `DECLINED`/`REJECTED` and append-only audit.
- Duplicate/stale commands do not issue another independent financial authorization; uncertain outcomes are not guessed to be failures.

## Scope Lock

**Include:** Kafka command handling, simulated PSP authorization path, durable state changes and integration tests.

**Exclude:** No capture/void; no multi-attempt timeout policy (M3-03), no completed parallelism/offset-tracker guarantees before M3-04/M3-05.

## Authoritative Sources

- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [DMODEL](../domain/relational-data-model.md)
- [STATES](../domain/payment-state-machines.md)
- [BR6](../requirement/BR-006-distinguish-technical-failure-from-decline.md)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)

## Acceptance Criteria

1. PSP approval/decline maps to distinct confirmed Payment and Operation states with correlated audit atomically.
2. Redelivery of a finalized operation makes no second PSP financial effect and preserves IDs.
3. Kafka polling is not blocked by PSP HTTP; **consumer auto-commit is disabled from the first consumer slice**, and no uncommitted/in-flight record is acknowledged merely because processing was dispatched. Full contiguous offset commits remain M3-05.

## Dependencies

- [M2-02](M2-02-recover-outbox-publication.md)
- M3 contract approval: fake PSP endpoints, provider identity/idempotency, response taxonomy and worker handoff/acknowledgment boundary.
