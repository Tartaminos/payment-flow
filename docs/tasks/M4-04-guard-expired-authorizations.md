# M4-04 — Expired Authorization Guard

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Prevent capture when authorization expiration has been authoritatively established.

## Required Behavior

- Before admitting/executing capture, evaluate agreed PSP evidence about authorization validity; confirmed expiry safely transitions `AUTHORIZED→EXPIRED` with audit.
- Return a distinct documented capture conflict (`AUTHORIZATION_EXPIRED` proposed) without contacting PSP to capture when expiry is confirmed.
- An unverified timestamp, PSP timeout or missing response is not evidence of expiry; never overwrite a confirmed CAPTURED state.

## Scope Lock

**Include:** Expiry detection/evidence handling, capture guard, atomic transition and race tests.

**Exclude:** No guessed expiration from clock alone, no automatic void-on-expiry, no refund/capture cancellation of a confirmed capture.

## Authoritative Sources

- [BR3](../requirement/BR-003-expired-authorizations.md)
- [BR4](../requirement/BR-004-idempotent-capture-requests.md)
- [STATES](../domain/payment-state-machines.md)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [DMODEL](../domain/relational-data-model.md)

## Acceptance Criteria

1. Authoritatively expired authorization cannot produce CAPTURE PSP request and is represented as `EXPIRED`, not `CANCELED`.
2. Uncertain expiry does not trigger an unfounded state change; existing CAPTURED remains unchanged.
3. Capture-vs-expiry race is resolved by DB eligibility guards and retains auditable evidence.

## Dependencies

- [M4-01](M4-01-admit-idempotent-full-capture.md)
- [M4-02](M4-02-execute-full-capture.md)
- M4 contract approval: trustworthy PSP expiry evidence, clock-skew/race rules and API error envelope.
