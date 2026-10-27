# REFACT-01 — Split Capture/Void Verticals Out of `payment`

**Status:** PROPOSED (2026-10-27). Not yet approved for Codex execution; requires explicit user approval before implementation, same as any other task in this folder.

## Objective

Reduce the file density of `jhsoftware.paymentsflow.payment` by extracting the capture and void verticals into their own subpackages, with no behavior change.

## Required Behavior

- `CaptureAdmissionService`, `CaptureAdmissionRepository`, `CaptureAdmissionCommand`, `CaptureAuthorizationValidityClient`, `CaptureAuthorizationValidityProperties`, `CaptureRequestHasher`, `CaptureValidityResponseBody`, `AuthorizationExpiredException`, `AuthorizationValidityEvidence`, `AuthorizationValidityStatus` move to `jhsoftware.paymentsflow.payment.capture`.
- `VoidAdmissionService`, `VoidAdmissionRepository`, `VoidAdmissionCommand`, `VoidRequestHasher` move to `jhsoftware.paymentsflow.payment.void_`.
- `PaymentProperties`, `CreatePaymentResponse`, shared exceptions (`PaymentNotFoundException`, `InvalidPaymentIdException`, `PspStatusUnavailableException`, `PaymentAlreadyCapturedException`, `PaymentNotEligibleException`, `PaymentOperationInProgressException`, `InvalidPaymentRequestException`, `IdempotencyKeyConflictException`, `ApiError`), `PaymentController`, `PaymentExceptionHandler`, `PaymentAdmission*`, `PaymentStatus*`, `PaymentConfiguration`, `CreatePaymentRequest` stay in `payment` root.
- Every class in the new subpackages that is called from outside its own subpackage (notably from `PaymentController`/`PaymentExceptionHandler` in `payment` root, and from `processor`) becomes `public`, as a deliberate, minimal, named surface. No class moves to `public` without an actual external caller.
- No endpoint, request/response shape, persistence behavior, transaction boundary, or test expectation changes as a result of this move.

## Scope Lock

**Include:** Package moves, import updates, the minimal required visibility widening (package-private → public) for classes with real external callers, and updating existing tests' imports/package declarations to match.

**Exclude:** Any change to `processor` or `outbox` packages, any behavioral/logic change in capture or void flows, any new abstraction, interface, or helper not already present, and any change to the `psp-simulator` module.

## Authoritative Sources

- [ADR-0001](../adr/0001-separate-payment-and-payment-operations.md): existing precedent for separating Payment from PaymentOperation concerns, consistent with further splitting operation-type verticals.
- Architecture review (2026-10-27, ad hoc, not yet a committed ADR): confirmed `CaptureAdmission*`/`VoidAdmission*` share no direct coupling with each other beyond `PaymentProperties`, `CreatePaymentResponse` and the common exceptions listed above, making this the lowest-risk package split in the codebase; `processor` was evaluated and explicitly rejected for this treatment due to extensive package-private cross-references between its retry/quarantine/reconciliation/PSP-client concerns.

## Acceptance Criteria

1. `jhsoftware.paymentsflow.payment.capture` and `jhsoftware.paymentsflow.payment.void_` exist, each containing only the classes listed above, with no other code moved.
2. The full existing test suite (unit + PostgreSQL/Testcontainers integration tests covering capture and void) passes unmodified in behavior, with only package/import references updated.
3. A diff review shows zero classes outside the two new subpackages were widened to `public` as a side effect, and every class made `public` has at least one caller outside its own subpackage.
4. `PaymentController`'s and `PaymentExceptionHandler`'s public HTTP contract (request/response bodies, status codes, error payloads) is byte-for-byte unchanged.

## Dependencies

- None.
