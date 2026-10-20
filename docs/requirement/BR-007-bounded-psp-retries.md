# BR-007 — Bounded and safe PSP HTTP attempts

- **Type:** Business rule / integration reliability requirement
- **Status:** Accepted (behavior and MVP timeout schedule); implementation details pending
- **Scope:** HTTP communication with the simulated Payment Service Provider (PSP)
- **Related:** [BR-002 — Unknown outcomes](BR-002-uncertain-psp-outcomes.md), [BR-005 — Reconciliation after 24h](BR-005-reconciliation-after-24h-unknown.md), [BR-006 — Distinguish technical failure](BR-006-distinguish-technical-failure-from-decline.md), [BR-008 — Block conflicting financial operations](BR-008-block-conflicting-financial-operations.md)

## Business context

PSP connections can time out, return ambiguous errors, or become unavailable. Unbounded retries consume resources and risk repeating financial side effects. However, exhaustion of HTTP attempts does **not** prove a charge/authorization was not executed.

## Requirement

> PSP financial HTTP interactions must have a bounded number of attempts and an increasing per-request timeout, without blocking the caller's PaymentFlow API request. Repeated external financial actions are allowed **only** when the PSP contract guarantees safe idempotent retry for the **same logical operation identity**.

## Accepted MVP attempt and timeout schedule

| HTTP attempt | Maximum duration of that individual HTTP request |
| --- | ---: |
| 1 | **5 seconds** |
| 2 | **10 seconds** |
| 3 | **20 seconds** |

- **Three HTTP attempts maximum in total**, including the first request; not three retries after the first.
- These are **individual HTTP request timeouts**, **not** waiting intervals between attempts. The cumulative request-timeout budget is up to **35 seconds**, excluding scheduling/backoff delays, overhead, and earlier responses.
- A separate **retry delay/backoff policy remains to be defined**, including when to schedule and which error classes are retryable.
- PSP calls/retries occur in an **asynchronous worker**, not inside the original client-facing `POST /payments` response flow. The proposed API response can acknowledge acceptance while processing continues; exact endpoints and HTTP response contracts are separate design decisions.
- Actual retry execution must be safe: **if the PSP contract does not offer reliable idempotent financial retries for the same operation identity, do not blindly repeat the financial request**. Query the original operation's status when supported or transition to `UNKNOWN` and reconcile.

## Expected behavior

1. Create/retain one `PaymentOperation` representing one **logical financial operation**, independently of its number of HTTP attempts.
2. When a safe retry is permitted, preserve the **same PSP idempotency/operation identity** across all attempts.
3. A definitive recorded PSP outcome immediately ends the attempt loop and follows the domain mapping.
4. A proven technical rejection with **authoritative evidence of non-execution** may result in `PaymentOperationStatus.FAILED` (BR-006).
5. An ambiguous result, including timeouts with unknown PSP execution outcome, must not be classified as `FAILED`. When safe retries are exhausted (or unsafe/unavailable), the operation becomes or remains `UNKNOWN`.
6. Record attempt count, per-attempt timeout configuration, failure reason, timestamps and correlation with the logical operation for audit/diagnostics.
7. The API request must not remain open across the PSP retry sequence; worker orchestration must not occupy application request threads during waiting/backoff.
8. Existing protection against duplicate requests and conflicting new operations remains in force (BR-001, BR-004, BR-008).

## Scenario: capture timeout

```text
Payment: AUTHORIZED
PaymentOperation #456: CAPTURE / PROCESSING
PSP operation key: capture-456

Attempt #1: timeout = 5s  -> timeout, outcome uncertain
Attempt #2: timeout = 10s -> timeout, only if PSP supports safe retry
Attempt #3: timeout = 20s -> timeout, only if PSP supports safe retry

No authoritative PSP result:
  Payment remains AUTHORIZED
  PaymentOperation #456 becomes UNKNOWN
  diagnostic reason: RETRY_EXHAUSTED
  reconciliation/status resolution required
```

`RETRY_EXHAUSTED` is a **diagnostic reason**, not an extra `PaymentOperationStatus` and not evidence of PSP non-execution. Three timeout attempts do **not** create three PaymentOperations.

## Acceptance criteria

- **AC-001:** With retry-safe PSP conditions, the worker performs at most **3** financial HTTP attempts, using **5s**, **10s**, and **20s** as the respective request timeout budgets.
- **AC-002:** The API request returns without waiting for all PSP attempts.
- **AC-003:** Every permitted attempt retains the same PSP financial operation identity/idempotency key.
- **AC-004:** When all attempted requests time out ambiguously, the operation is `UNKNOWN`, not `FAILED`, and the Payment retains its last confirmed state.
- **AC-005:** If retry safety is not established by the PSP contract, the worker does not issue an additional charge/authorization request solely because of a timeout.
- **AC-006:** A definitive result immediately stops further attempts.
- **AC-007:** Attempt counts, timeout budgets, outcomes, and audit correlation are available for investigation.

## Open design decisions

- Retry delay and backoff/jitter between calls; distinguish these from the accepted 5/10/20-second **request timeout** schedule.
- Which PSP error classes permit safe retry, and the PSP simulator's precise idempotency guarantees.
- Distinct connection vs. response timeout implementation, scheduling, and cancellation behavior.
- Worker delivery / reservation / locking semantics and event contracts.
- Status-query behavior and budget for resolving a potentially already executed operation (not a new financial operation).
