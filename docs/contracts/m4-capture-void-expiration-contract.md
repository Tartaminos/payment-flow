# M4 — Capture, Void and Authoritative Authorization Expiry Contract

- **Status:** **PROPOSED — pending explicit user approval** (2026-10-24); M4-01/M4-04/M4-02/M4-03 remain PLANNED and non-executable.
- **Authority:** [M3 contract proposal](m3-payment-processor-contract.md), [BR-003](../requirement/BR-003-expired-authorizations.md), [BR-004](../requirement/BR-004-idempotent-capture-requests.md), [BR-008](../requirement/BR-008-block-conflicting-financial-operations.md), [state machines](../domain/payment-state-machines.md), [M1 API conventions](m1-payment-creation-contract.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md).

## API admission (M4-01 / M4-03)

- Full capture: **`POST /payments/{paymentId}/capture`** with mandatory case-sensitive `Idempotency-Key` (same 1–150 character rule as M1), **no request body** and amount implicitly equal to the authorized full amount in minor units. Reject partial/extra body with `400 INVALID_PAYMENT_REQUEST`.
- Void: **`POST /payments/{paymentId}/void`** with same header/no-body rule; acts only on a not-yet-captured, still active authorization. No refunds, partial captures, reversals or automated void-on-expiry.
- Merchant is the same configured UUID as M1, never a user-supplied merchant header.
- **Idempotency scope:** unique `(merchant_id, operation_scope, key)` where scopes are `PAYMENT_CAPTURE` and `PAYMENT_VOID` (distinct from M1 `PAYMENT_CREATE`). Hash canonical `paymentId`, operation kind and derived full authorized amount/currency; no raw JSON hashing. Same key + identical parameters returns the original operation/payment identities/status and **202** even if its state has since become terminal; same key + differing parameters returns **409 IDEMPOTENCY_KEY_CONFLICT**. No automatic key expiry in the MVP.
- **First admitted call:** `202 Accepted` with `{paymentId,operationId,status,operationStatus}`, using the current Payment state (typically `AUTHORIZED`) and `operationStatus=PENDING`. A 202 does **not** claim the PSP has captured/voided anything.
- Missing/invalid ID/header/body → `400`. Unknown Payment → `404 PAYMENT_NOT_FOUND`. Existing different-key capture against `CAPTURED` → **`409 PAYMENT_ALREADY_CAPTURED`**; conflicting unresolved operation → **`409 PAYMENT_OPERATION_IN_PROGRESS`**; invalid state → `409 PAYMENT_NOT_ELIGIBLE`. Confirmed expiry → **`409 AUTHORIZATION_EXPIRED`** (BR-003). Unexpected infrastructure outage yields safe 5xx, never 202 for uncommitted admission. Error envelope follows M1 `{code,message}`.

## PSP simulation and authoritative validity evidence

1. Add fake PSP endpoints `POST /simulator/operations/capture` and `POST /simulator/operations/void` with stable **distinct original CAPTURE or VOID PaymentOperation UUID** as `pspOperationId`, plus an `authorizationPspOperationId` referencing the confirmed original AUTHORIZE PSP identity. PSP-side idempotency uniqueness and persisted outcomes use the M3 rules; duplicate same identity cannot charge/void twice and mismatched parameters conflict.
2. `GET /simulator/operations/{authorizationPspOperationId}` reports confirmed **authorization validity `ACTIVE` or `EXPIRED`** separately from whether authorization initially succeeded. The fake PSP stores the authoritative validity/expiry decision and the timestamp/evidence reference; no PaymentFlow local timestamp or timeout alone can prove provider expiry.
3. **M4-01 admits only if there is affirmative trusted PSP `ACTIVE` evidence** at admission; if evidence is absent/ambiguous, fail closed: `503 PSP_STATUS_UNAVAILABLE`, with no CAPTURE/Outbox created. M4-01 does not need to implement the transition to EXPIRED yet, but cannot admit unverified work.
4. **M4-04** refines the initial guard: with PSP's definitive `EXPIRED` evidence, atomically transition `Payment AUTHORIZED→EXPIRED` plus append-only reason/evidence audit, then reject capture with `409 AUTHORIZATION_EXPIRED`; explicitly protect the check against a concurrent completed capture. Keep unknown evidence unchanged. It also provides a reusable server-side expiry eligibility check for the worker.
5. **M4-02 rechecks validity immediately before PSP capture** via the guard delivered in M4-04, and the simulated PSP itself must reject an expired capture even if expiry races after the check (authoritative PSP verdict). Confirmed provider expiry before side effect resolves capture as non-execution/rejection with current Payment `EXPIRED` where permitted. A lost PSP response is **UNKNOWN**, never assumed captured nor expired.
6. An expiry result arriving after a confirmed `CAPTURED` Payment cannot overwrite it. If CAPTURE operation is UNKNOWN, do not change Payment `AUTHORIZED→EXPIRED` until the potentially completed capture has been reconciled; UNKNOWN conflicting-operation block takes precedence.

## Capture (M4-01 → M4-04 → M4-02)

- M4-01 first admission atomically creates existing-Payment `CAPTURE:PENDING`, scoped idempotency, audit and one pending Outbox intent, without PSP capture in the client HTTP request. **Admission must obtain row/conditional DB eligibility checks, not only a previously fetched status**. New conflicting CAPTURE/VOID is rejected while `PENDING`, `PROCESSING` or `UNKNOWN` operations make the request unsafe.
- M4-04's expiry guard is required **before M4-02**: verify before admitting/executing, preserve provider evidence and prevent an expired authorization from reaching the PSP.
- M4-02 consumes the original generic `ExecutePaymentOperation`, rechecks authoritative DB state, PSP validity and stable financial identity. Confirmed full capture → `Payment:CAPTURED` and `CAPTURE:SUCCEEDED`, plus audit in one DB transaction. Explicit provider rejection → `CAPTURE:REJECTED` without marking Payment CAPTURED. Proven no-effect → `CAPTURE:FAILED`; ambiguity → `CAPTURE:UNKNOWN` (Payment remains last confirmed AUTHORIZED) and durable reconciliation job.
- All CAPTURE HTTP retries count against that operation's **same total 3 attempts**, with 5s/10s/20s request budgets and no reset after Kafka replay. **M4-02 must extend M3's existing independent reconciliation scanner to perform active, durably resumable PSP status lookup and minimal definitive-outcome resolution for CAPTURE UNKNOWN before its Kafka offset can be acknowledged.** Preserve the original PSP operation ID. M6-01 later completes/generalizes reconciliation semantics and evidence handling for all operation types.

## Void (M4-03)

- VOID admission/execution is an **eligible authorized-payment cancellation**, not expiry and never a refund. Use scoped idempotency and single-transaction `VOID:PENDING` + audit + Outbox, with the same conflict/eligibility guards as capture.
- Confirmed void → `Payment:CANCELED` and `VOID:SUCCEEDED` with audit in one transaction. Explicit refusal or proven non-execution does not produce CANCELED. Timeout/unknown PSP outcome → `VOID:UNKNOWN`, Payment retains AUTHORIZED and independent reconciliation responsibility exists.
- Reject VOID if Payment is CAPTURED, EXPIRED, CANCELED, or has conflicting pending/processing/unknown financial work. PSP-side void response is authoritative and may reject a changed authorization state; preserve original operation ID and evidence.
- No independent PSP action may be retried merely due to consumer redelivery; only provider-idempotent attempts with the same original identity are permitted. **M4-03 extends that same active, independently recoverable scanner to VOID UNKNOWN with original PSP ID before allowing UNKNOWN offset acknowledgment**; mere passive job storage does not qualify.

## Concurrency, transition proof and tests

- Transactional/admission locks or conditional updates must protect Payment and conflicting PaymentOperation eligibility. Stale workers cannot overwrite successful captured/voided/expired outcomes. Every state transition and audit event commit together.
- Verify: same-key equivalent/conflicting capture/void, different-key after CAPTURED, simultaneous capture-vs-capture, capture-vs-void, capture-vs-expiry, active/expired/unavailable PSP validity, duplicate Kafka delivery, lost-response UNKNOWN and reconciliation using original ID. No simulated cardholder secrets.
- **Proposed task sequence:** [M4-01](../tasks/M4-01-admit-idempotent-full-capture.md) → [M4-04](../tasks/M4-04-guard-expired-authorizations.md) → [M4-02](../tasks/M4-02-execute-full-capture.md) → [M4-03](../tasks/M4-03-void-authorized-payment.md). M4-01 is **not** production-complete without M4-04 and M4-02, but it must fail closed on unverified validity from day one.

**Approval gate:** This contract and the four slices remain proposals until the user approves exact endpoint/error/PSP expiry semantics. No code execution is authorized by publication.
