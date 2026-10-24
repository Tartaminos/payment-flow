# M4-03 — Void Authorized Payment

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Admit and execute eligible void/cancellation against one existing authorization.

## Required Behavior

- Provide a proposed void endpoint creating one `VOID:PENDING` plus audit/Outbox only for an eligible authorization; replays keep one logical VOID identity.
- Execute the existing operation through generic processing; `Payment:CANCELED` occurs only after PSP confirms the void.
- Block void of captured/expired or business-ineligible payments and new conflicting operations while another result is unresolved; never confuse expiry with an explicit void. **Extend active independent PSP status lookup and recovery to VOID UNKNOWN before Kafka offset acknowledgment**; use the original VOID operation ID.

## Scope Lock

**Include:** Void admission, simulated provider call, safe replay/conflict behavior and resulting audit/test cases.

**Exclude:** No refund, reversal of completed capture, PSP webhook implementation or automatic expiry-triggered void.

## Authoritative Sources

- [M4 proposed implementation contract](../contracts/m4-capture-void-expiration-contract.md) (**PROPOSED — review before implementation**)
- [BR8](../requirement/BR-008-block-conflicting-financial-operations.md)
- [BR2](../requirement/BR-002-uncertain-psp-outcomes.md)
- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [STATES](../domain/payment-state-machines.md)
- [DMODEL](../domain/relational-data-model.md)

## Acceptance Criteria

1. Successful confirmed void produces one `VOID:SUCCEEDED` and `Payment:CANCELED` with audit.
2. Replay, concurrency and conflicting UNKNOWN operation never start a new independent void.
3. Invalid/expired/captured Payment is rejected without a PSP void; timeout alone never becomes `CANCELED`, and VOID UNKNOWN remains actively recoverable without needing original Kafka replay.

## Dependencies

- [M4-02](M4-02-execute-full-capture.md)
- M4 contract approval: void endpoint, legal states, idempotency scope and PSP void semantics.
