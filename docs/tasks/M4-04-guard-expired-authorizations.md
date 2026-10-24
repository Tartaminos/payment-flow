# M4-04 — Expired Authorization Guard

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Enforce authoritative authorization-expiry eligibility before a CAPTURE operation is admitted.

## Required Behavior

- Before **admitting** capture, evaluate agreed authoritative PSP evidence about authorization validity; confirmed expiry safely transitions `AUTHORIZED→EXPIRED` with audit. Expose/reuse that same guard for the later worker-side check in M4-02.
- Return a distinct documented capture conflict (`AUTHORIZATION_EXPIRED` proposed) without contacting PSP to capture when expiry is confirmed.
- An unverified timestamp, PSP timeout or missing response is not evidence of expiry; never overwrite a confirmed CAPTURED state.

## Scope Lock

**Include:** Capture-admission expiry detection/evidence, reusable eligibility guard, atomic transition and race tests; worker-side enforcement is validated by M4-02.

**Exclude:** No guessed expiration from clock alone, no automatic void-on-expiry, no refund/capture cancellation of a confirmed capture.

## Authoritative Sources

- [M4 proposed implementation contract](../contracts/m4-capture-void-expiration-contract.md) (**PROPOSED — review before implementation**)
- [BR3](../requirement/BR-003-expired-authorizations.md)
- [BR4](../requirement/BR-004-idempotent-capture-requests.md)
- [STATES](../domain/payment-state-machines.md)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [DMODEL](../domain/relational-data-model.md)

## Acceptance Criteria

1. A capture request for an authoritatively expired authorization creates **no CAPTURE operation or Outbox intent**, records `EXPIRED` with audit and returns the agreed error; M4-02 must also apply the guard before PSP execution.
2. Uncertain expiry does not trigger an unfounded state change; existing CAPTURED remains unchanged.
3. Capture-vs-expiry race is resolved by DB eligibility guards and retains auditable evidence.

## Dependencies

- [M4-01](M4-01-admit-idempotent-full-capture.md)
- M4 contract approval: trustworthy PSP expiry evidence, clock-skew/race rules and API error envelope.
