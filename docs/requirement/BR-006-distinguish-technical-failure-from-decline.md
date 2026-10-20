# BR-006 — Distinguish PSP technical failure from payment decline

- **Type:** Business rule
- **Status:** Accepted at the behavioral level; PSP outcome model pending
- **Scope:** Authorization, payment outcomes, and PSP integration failures
- **Related:** [Payment state machines](../domain/payment-state-machines.md), [BR-002](BR-002-uncertain-psp-outcomes.md)

## Business context

A PSP's definitive decision to decline a card authorization is not equivalent to a technical failure that prevented the operation from executing.

A connection timeout is neither: it creates uncertainty rather than a confirmed failure.

## Requirement

> PaymentFlow must distinguish financial rejection (`PaymentStatus.DECLINED`), definitive technical execution failure (`PaymentOperationStatus.FAILED`), and unknown outcome (`PaymentOperationStatus.UNKNOWN`).

## Expected behavior

### A. Authorization is definitively declined

Example PSP result: `DECLINED` with reason `INSUFFICIENT_FUNDS`.

- `Payment.status = DECLINED`.
- `PaymentOperation(type=AUTHORIZE).status = SUCCEEDED` **when SUCCEEDED means that the interaction obtained a definitive business answer**.
- The operation must additionally record a distinguishable business decision/result `DECLINED` (exact data model remains open).

Here `SUCCEEDED` never means that the card was approved.

### B. Definitive technical failure

Example: PSP reliably confirms that it rejected the request due to invalid merchant credentials and did **not** perform a financial operation.

- `PaymentOperation(type=AUTHORIZE).status = FAILED`.
- `Payment.status` remains `CREATED` (last confirmed business fact).
- Do not automatically retry without addressing the root cause.

A generic HTTP `500` alone does **not** establish that a financial operation did not execute.

### C. Unknown outcome

Example: request times out, response is lost, or PSP provides an ambiguous response.

- `PaymentOperation.status = UNKNOWN`.
- Payment keeps its last confirmed business status.
- Follow [BR-002](BR-002-uncertain-psp-outcomes.md) and, after 24 hours, [BR-005](BR-005-reconciliation-after-24h-unknown.md).

## Acceptance criteria

- **AC-001:** A definitive card decline results in `PaymentStatus.DECLINED`, not technical `FAILED`.
- **AC-002:** A definitive technical rejection with proof of no execution results in `PaymentOperationStatus.FAILED` and does not falsely mark the card as declined.
- **AC-003:** A timeout or ambiguous HTTP error alone cannot be classified as definitive failure.
- **AC-004:** Operation processing status and the PSP's business decision are distinguishable in audit records.
- **AC-005:** A technical failure does not erase the last confirmed Payment business status.

## Open design decisions

- Concrete representation of PSP business outcomes and error codes, distinct from `PaymentOperationStatus`.
- Failure classification and which technical errors permit safe retries.
- PSP simulator contracts that supply authoritative evidence of no execution.
