# ADR-0003 — Distinguish financial success, business rejection and technical failure

- **Status:** Accepted
- **Date:** 2026-10-20
- **Scope:** `PaymentOperationStatus` semantics and PSP results
- **Related:** [ADR-0001](0001-separate-payment-and-payment-operations.md), [BR-006](../requirement/BR-006-distinguish-technical-failure-from-decline.md), [Payment state machines](../domain/payment-state-machines.md)

## Context

Our initial operation model used `SUCCEEDED` to mean "a definitive PSP response was received," including an explicit card decline. This is ambiguous: a query for successful financial authorizations would still have to inspect a second field to exclude declines.

`Payment` and `PaymentOperation` are separate: the former records business payment facts; the latter describes the outcome of one **logical financial PSP operation**, independent of the number of HTTP attempts. We also need to distinguish business rejection from confirmed technical non-execution and uncertain timeouts.

## Decision

Expand `PaymentOperationStatus` to six values:

| Value | Meaning |
| --- | --- |
| `PENDING` | Logical financial operation registered and awaiting execution. |
| `PROCESSING` | Execution underway; final result not yet established. |
| `SUCCEEDED` | PSP **confirmed that the requested financial operation succeeded** (e.g., authorization granted or capture completed). |
| `REJECTED` | PSP definitively rejected the requested operation **for a business/domain reason** (e.g., card declined). No successful financial effect for that requested operation. |
| `FAILED` | Definitive technical/integration failure with trustworthy confirmation that the requested financial effect did not occur. |
| `UNKNOWN` | The PSP outcome cannot yet be established; timeout or ambiguous error is not proof of failure. |

For the MVP:
- An authorization approval maps to `Payment.status = AUTHORIZED` and `AUTHORIZE.status = SUCCEEDED`.
- An authorization decline maps to `Payment.status = DECLINED` and `AUTHORIZE.status = REJECTED`.
- A completed capture maps to `Payment.status = CAPTURED` and `CAPTURE.status = SUCCEEDED`.
- A definitive technical rejection with proven non-execution maps to `PaymentOperation.status = FAILED`, retaining the last confirmed `Payment.status`.
- A timeout/ambiguous result maps to `PaymentOperation.status = UNKNOWN`, retaining the last confirmed `Payment.status`.

Keep PSP-specific details such as `INSUFFICIENT_FUNDS` / `AUTHORIZATION_EXPIRED` as **result/reason codes**, distinct from these coarse lifecycle statuses.

`REJECTED` is used at the operation level because it applies beyond card authorization (e.g., a PSP can reject a capture or void request). `DECLINED` remains the established Payment-level authorization result.

## Alternatives

1. **Keep SUCCEEDED for any definitive PSP answer.** Rejected because it conflates successful operation with successfully *receiving a response*.
2. **Add DECLINED to PaymentOperationStatus.** Rejected because it is too specific for operations other than authorization.
3. **Add REJECTED, retaining separate FAILED and UNKNOWN.** Accepted for clearer domain semantics and simpler queries and constraints.

## Consequences

- Any documentation that classifies an explicitly declined authorization as `AUTHORIZE: SUCCEEDED` is superseded by this decision.
- A successful-authorization uniqueness rule may filter `type = AUTHORIZE` and `status = SUCCEEDED`. This does **not** replace admission control for concurrent `PENDING` / `PROCESSING` / `UNKNOWN` operations.
- State transitions now include `PROCESSING -> REJECTED` and `UNKNOWN -> REJECTED` when authoritative evidence arrives; no timeout or retry exhaustion may imply `REJECTED` or `FAILED`.
- Statuses alone do not determine the provider's detailed error/rejection reason.
- Exact retry, rollback and terminal-state transition matrix remains to be specified.

## Scope limits

This ADR changes the meaning of operation outcomes, not Payment-level statuses, the number of services, Kafka semantics, PSP API contracts or database migrations. Only documentation is maintained at this stage.
