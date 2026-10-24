# M4-02 — Execute Confirmed Full Capture

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Execute the accepted full CAPTURE operation through the generic worker without double capture.

## Required Behavior

- Consume the existing CAPTURE command, revalidate the Payment's authorization **and authoritative expiry evidence** using the guard established by M4-04, then call the fake PSP under the same stable operation identity **only if eligible**. If expiration is confirmed, do not call the PSP; if validity is unresolved, follow the M4 contract's safe deferral/resolution policy rather than assuming eligibility.
- Only a confirmed full PSP capture transitions Payment to `CAPTURED` and CAPTURE operation to `SUCCEEDED`; update audit in the same transaction.
- Duplicate or out-of-order records never cause a second charge; ambiguous outcomes remain UNKNOWN and cannot be recorded as success.

## Scope Lock

**Include:** CAPTURE branch of generic processing, PSP simulator behavior, durable outcome/audit and failure tests.

**Exclude:** No separate capture-only Kafka pipeline, partial capture, settlement or refunds.

## Authoritative Sources

- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [A5](../adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md)
- [BR4](../requirement/BR-004-idempotent-capture-requests.md)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [BR7](../requirement/BR-007-bounded-psp-retries.md)
- [STATES](../domain/payment-state-machines.md)

## Acceptance Criteria

1. Confirmed full capture of an eligible, nonexpired authorization transitions `AUTHORIZED→CAPTURED` exactly once and persists successful operation/audit atomically.
2. Duplicate CAPTURE command causes no additional financial attempt when original result is confirmed.
3. Confirmed expiry or unresolved validity cannot result in an unsafe CAPTURE call; unknown/timed-out PSP results do not falsely capture or discard the original operation identity.

## Dependencies

- [M4-01](M4-01-admit-idempotent-full-capture.md)
- [M4-04](M4-04-guard-expired-authorizations.md)
- M4 contract approval: fake PSP capture response/evidence and financial idempotency semantics.
