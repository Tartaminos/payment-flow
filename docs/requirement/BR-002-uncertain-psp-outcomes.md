# BR-002 — Safe handling of uncertain PSP outcomes

- **Type:** Business rule
- **Status:** Agreed at the behavioral level; implementation details pending
- **Scope:** Payment operations sent to an external Payment Service Provider (PSP)
- **Related:** [BR-001 — Idempotent payment requests](BR-001-idempotent-payment-requests.md)

## Business context

PaymentFlow requests a financial operation from a PSP. The PSP may complete the operation but fail to deliver its response because of a timeout, network interruption, or service crash.

A missing response is **not evidence that the payment failed**.

## Requirement

> When the result of a PSP financial operation is unknown, PaymentFlow must preserve its identity and avoid blindly issuing another independent charge. It must attempt to resolve the original operation's outcome before taking further financial action.

### Expected behavior

1. PaymentFlow identifies each external financial operation with a stable operation identity; when supported by the PSP, use a stable PSP idempotency key for safe retries.
2. If the PSP request times out or its response is lost, the outcome is recorded as **unknown / pending confirmation**, not immediately as a definitive failure.
3. PaymentFlow attempts to determine the operation result using an available reliable mechanism, such as a PSP status query or an authenticated PSP webhook.
4. Retrying a PSP operation, when the provider supports safe retries, must reuse the **same PSP operation identity/idempotency key**, not invent a new charge identity.
5. If the outcome remains unknown, the payment stays unresolved until additional evidence, reconciliation, or an explicit recovery workflow determines the next safe action.
6. A later PSP confirmation must be correlated to the original operation and must not result in a duplicate financial effect.

## Example failure sequence

```text
Client                PaymentFlow                  PSP
  |                         |                       |
  |---- POST /payments ---->|                       |
  |                         |--- authorize/capture ->|
  |                         |                       | operation executed
  |                         |<--- response lost ------X
  |                         |
  |                   outcome UNKNOWN
  |                         |
  |                  query status / await webhook
  |                         |
  |                  reconcile original operation
```

A repeated client request using the original `Idempotency-Key` is governed by [BR-001](BR-001-idempotent-payment-requests.md). That client key and the PSP-side operation identity serve **different boundaries** and should not be conflated.

## Acceptance criteria

- **AC-001:** Given that the PSP executes a charge but the response is lost, PaymentFlow does not automatically mark the operation as `FAILED`.
- **AC-002:** Given an uncertain PSP outcome, PaymentFlow does not initiate a new, independently identified charge as an automatic recovery action.
- **AC-003:** Given a PSP webhook or successful status lookup for the original operation, PaymentFlow can associate the result with the existing payment and resolve the unknown outcome.
- **AC-004:** Given a retry of the same PSP request (where supported), the retry uses the previously assigned stable PSP operation identity/idempotency key.
- **AC-005:** Given repeated delivery of a PSP result, PaymentFlow must not apply the financial result twice.

## Boundaries and open design decisions

**Established:** Timeout is not equivalent to failure; uncertain operations require correlation and safe recovery.

**To decide during domain/architecture design:**
- Final payment and external-operation status names (e.g., `PENDING_CONFIRMATION` is only a candidate).
- PSP contract, including idempotency guarantees, query API, and webhook authentication.
- Strategy for reconciling long-running unknown operations, timeouts, and manual intervention.
- Concurrency control for PSP callbacks, polling, and Kafka consumer redelivery.
- Authorization versus capture operation identities and their separate idempotency boundaries.

**Guarantee limitation:** If a PSP does not support idempotent operations **and** provides no reliable means to query or reconcile outcomes, PaymentFlow cannot unilaterally guarantee exactly-once external financial effects. The simulated PSP contract will need to define which guarantees are available.
