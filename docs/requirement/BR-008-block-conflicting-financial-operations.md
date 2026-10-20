# BR-008 — Block new conflicting financial operations while outcomes remain unresolved

- **Type:** Business rule
- **Status:** Accepted for MVP (strict block; no override path)
- **Scope:** PaymentOperation creation, concurrent PSP actions, idempotency and reconciliation
- **Related:** [BR-002 — Unknown outcomes](BR-002-uncertain-psp-outcomes.md), [BR-004 — Idempotent capture](BR-004-idempotent-capture-requests.md), [BR-005 — Reconciliation](BR-005-reconciliation-after-24h-unknown.md), [BR-007 — Bounded PSP retries](BR-007-bounded-psp-retries.md)

## Business context

A timeout may mean that a PSP charge or authorization occurred even though PaymentFlow has no conclusive result. Starting a new, independently identified conflicting financial operation can repeat the external effect.

A repeated HTTP attempt with the **same logical operation identity** is *not* a new PaymentOperation, provided the PSP explicitly supports safe, idempotent retries.

## Requirement

> While an earlier PaymentOperation for the same payment has an unresolved execution outcome, PaymentFlow must not create or dispatch another **independent, conflicting financial operation**. The existing operation must be resolved first.

For the MVP:

- An `UNKNOWN` financial operation blocks creating and dispatching a new conflicting `AUTHORIZE`, `CAPTURE`, or `VOID` for the same Payment.
- In-flight `PENDING` and `PROCESSING` operations also require transactional concurrency controls so concurrent clients cannot dispatch separate conflicting operations.
- Idempotent replay of an **already existing logical operation** must resolve to its existing identity and result (or unresolved status), not create another operation.
- A definitive PSP result may release the block **only if** the Payment's resulting business state permits the next operation. For example, confirmed authorization may allow capture, but a confirmed `CAPTURED` payment does not allow another full capture.
- The block does **not** prevent safe status lookup, webhook processing, reconciliation, or an idempotent retry of the **same** PSP operation when supported. None of those actions authorizes another independent charge.

## Exceptions

An administrative exception or API contract may eventually define a justified override with explicit authorization, independent evidence about the prior PSP operation, and a durable audit trail.

**This override is explicitly out of scope for the MVP**. A free-text justification sent by the API client is not sufficient to waive the block. No public `force=true` / bypass flag or exception endpoint is authorized by this requirement.

## API behavior (to finalize later)

A new conflicting operation should be rejected without contacting the PSP. A `409 Conflict` response and a stable error code such as `PREVIOUS_OPERATION_UNRESOLVED` are **proposals**, not yet an approved API contract.

A same-key replay is resolved using existing idempotency behavior; do not reject it as though it were a genuinely new financial operation.

## Acceptance criteria

- **AC-001:** With an unresolved `CAPTURE: UNKNOWN`, a different-key request for a new capture is blocked with no new PSP financial request.
- **AC-002:** With an unresolved `AUTHORIZE: UNKNOWN`, a new conflicting authorization is blocked pending authoritative resolution.
- **AC-003:** Concurrent requests cannot bypass the restriction by reading the prior operation status at the same time; the implementation provides atomic admission/locking constraints.
- **AC-004:** Replaying a previously created operation with its original idempotency identity never creates a second logical operation.
- **AC-005:** PSP status queries, reconciliation and authenticated webhooks may still resolve the original operation.
- **AC-006:** No manual-override / justification bypass is available in the MVP.
- **AC-007:** Following a definitive result, subsequent operations are allowed only when valid under the Payment state machine and the other business rules.

## Open design decisions

- Exact conflict/admission rules across operation types, including cancellation while a prior operation is processing.
- Database constraints and transactional ordering to enforce concurrent admission safely.
- PSP reconciliation and business-facing error contracts.
- A future independently authorized override design, **not** part of the MVP.
