# BR-006 — Distinguish PSP technical failure from payment decline

- **Type:** Business rule
- **Status:** Accepted, refined by [ADR-0003](../adr/0003-distinguish-financial-success-from-rejection.md); detailed PSP error taxonomy pending
- **Scope:** Authorization, payment outcomes, and PSP integration failures
- **Related:** [Payment state machines](../domain/payment-state-machines.md), [BR-002](BR-002-uncertain-psp-outcomes.md)

## Business context

A PSP's definitive decision to decline a card authorization is not equivalent to a technical failure that prevented the operation from executing.

A connection timeout is neither: it creates uncertainty rather than a confirmed failure.

## Requirement

> PaymentFlow must distinguish definitive business rejection (`PaymentOperationStatus.REJECTED`), successful financial execution (`PaymentOperationStatus.SUCCEEDED`), definitive technical non-execution (`PaymentOperationStatus.FAILED`), and uncertain outcome (`PaymentOperationStatus.UNKNOWN`). Card authorization rejection also maps to `PaymentStatus.DECLINED`.

## Expected behavior

### A. Authorization is definitively declined

Example PSP result: `DECLINED` with reason `INSUFFICIENT_FUNDS`.

- `Payment.status = DECLINED`.
- `PaymentOperation(type=AUTHORIZE).status = REJECTED`.
- The operation retains the provider's specific rejection reason, e.g. `psp_result_code = INSUFFICIENT_FUNDS`.

`SUCCEEDED` is reserved for a **confirmed successful financial operation**; receiving a definitive decline does not count as operation success.

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

- **AC-001:** A definitive card decline results in `PaymentStatus.DECLINED` and `PaymentOperationStatus.REJECTED`, never `SUCCEEDED` or technical `FAILED`.
- **AC-002:** A definitive technical rejection with proof of no execution results in `PaymentOperationStatus.FAILED` and does not falsely mark the card as declined.
- **AC-003:** A timeout or ambiguous HTTP error alone cannot be classified as definitive failure.
- **AC-004:** Audit records distinguish financially successful operations (`SUCCEEDED`) from definitively rejected business operations (`REJECTED`) and include sanitized PSP-specific reason codes where available.
- **AC-005:** A technical failure does not erase the last confirmed Payment business status.

## Open design decisions

- Concrete provider-specific business result/rejection/error-code taxonomy (`psp_result_code`, etc.) distinct from the standardized `PaymentOperationStatus` enum.
- Failure classification and which technical errors permit safe retries.
- PSP simulator contracts that supply authoritative evidence of no execution.
