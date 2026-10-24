# M3-02 — Read Payment Status

**Status:** APPROVED for implementation (2026-10-24). Run via the configured local Codex orchestration skill **only after prerequisite tasks are complete and independently verified**; documentation approval does not execute Codex.

## Objective

Expose the confirmed Payment state and associated operation status to the client.

## Required Behavior

- Provide the approved `GET /payments/{paymentId}` returning the persisted Payment's current business status and relevant operation identity/status.
- Do not confuse `Payment:CREATED` with authorization success or infer PSP outcomes from Kafka dispatch.
- Return a documented not-found/validation response for unknown or malformed payment IDs.

## Scope Lock

**Include:** Read-only API behavior and integration tests over persisted authorized, declined, created and unknown states.

**Exclude:** No client webhooks, pagination, refund endpoints or new financial transitions.

## Authoritative Sources

- [M3 approved implementation contract](../contracts/m3-payment-processor-contract.md) (**APPROVED**)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [STATES](../domain/payment-state-machines.md)
- [DMODEL](../domain/relational-data-model.md)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)

## Acceptance Criteria

1. Request for a known Payment shows its stored status and corresponding operation state after async processing.
2. Unknown payment does not fabricate a financial result or change data.
3. An unresolved operation is distinguishable from the last confirmed Payment state.

## Dependencies

- [M3-01](M3-01-process-simulated-authorization.md)
- Approved [M3 implementation contract](../contracts/m3-payment-processor-contract.md): GET endpoint existence, response/error schema and whether/how operation history is exposed.
