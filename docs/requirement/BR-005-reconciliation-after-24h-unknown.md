# BR-005 — Escalate PSP operations that remain UNKNOWN for 24 hours

- **Type:** Business rule
- **Status:** Accepted at the behavioral level; reconciliation workflow pending
- **Scope:** External PSP operations with unresolved outcome
- **Related:** [BR-002](BR-002-uncertain-psp-outcomes.md), [BR-007](BR-007-bounded-psp-retries.md), [BR-008](BR-008-block-conflicting-financial-operations.md)

## Business context

A PSP request may complete while its response is lost. Even after retries of safe status checks or delayed callbacks, PaymentFlow may not know whether authorization/capture occurred.

Automatically canceling or marking such an operation as failed after an arbitrary deadline could cause a mismatch between PaymentFlow and the PSP.

## Requirement

> A PaymentOperation that remains `UNKNOWN` for more than 24 hours must trigger a mandatory reconciliation/escalation workflow. It must **not** be automatically canceled or classified as definitively failed solely due to elapsed time.

## Expected behavior

1. Record when the operation first enters `UNKNOWN` (`unknown_since` or equivalent).
2. Attempt safe provider status resolution as defined by the PSP contract while keeping the external operation identity unchanged.
3. At the 24-hour threshold, request reconciliation/escalation; first perform or schedule another safe PSP status check where supported.
4. If the outcome remains unknown, flag the operation for reconciliation/manual investigation and retain its `UNKNOWN` result.
5. A definitive PSP result can resolve the original operation; only then may the appropriate Payment state transition be applied.
6. Neither Kafka redelivery nor the 24-hour deadline is permission to create a new independently identified charge, authorize, or capture.

## Acceptance criteria

- **AC-001:** At 24 hours unresolved, the operation is flagged for reconciliation/escalation.
- **AC-002:** An `UNKNOWN` operation is never automatically transitioned to `FAILED` or canceled solely due to its age.
- **AC-003:** Pending reconciliation does not overwrite the Payment's last confirmed business state.
- **AC-004:** A delayed authoritative provider result is correlated to the existing operation without duplicate financial effect.
- **AC-005:** Repeated escalation jobs or events must not create duplicate financial actions.

## Open design decisions

- Whether the 24-hour threshold starts from first `UNKNOWN`, dispatch time, or last PSP contact (proposal: first `UNKNOWN`).
- Alerting, escalation ownership, reconciliation scheduling, and business-facing status.
- What to do when a PSP cannot conclusively identify an operation and the dispute remains unresolved for longer periods.
- Whether reconciliation has its own workflow entity/status outside `PaymentOperationStatus`.
