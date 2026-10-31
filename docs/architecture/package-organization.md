# Java Package Organization

The accepted convention is recorded in [ADR-0024](../adr/0024-organize-java-packages-by-responsibility.md). This guide maps responsibilities to the implemented source tree.

## PaymentFlow Maven project

Sources are under `src/main/java/jhsoftware/paymentsflow`. `PaymentsflowApplication` stays at that root. Feature boundaries are preserved.

### `payment`

| Package | Responsibility and examples |
| --- | --- |
| `controller` | HTTP endpoints and error advice: `PaymentController`, `PaymentExceptionHandler`. |
| `dto` | API contracts: `CreatePaymentRequestDto`, `CreatePaymentResponseDto`, `PaymentStatusResponseDto`, `ApiErrorDto`. |
| `command` | Admission inputs: `PaymentAdmissionCommand`, `CaptureAdmissionCommand`, `VoidAdmissionCommand`. |
| `service` | Payment admission/status, capture/void admission and their request hashers. |
| `repository` | Transactional admission and status persistence. |
| `client` | Authorization validity lookup: `CaptureAuthorizationValidityClient` and its internal `CaptureValidityResponseDto`. |
| `config` | Payment configuration and properties, including authorization validity client properties. |
| `exception` | Payment request, eligibility, conflict and availability exceptions. |
| `model` | Authorization validity evidence and status. |

### `processor`

| Package | Responsibility and examples |
| --- | --- |
| `config` | Consumer, retry/quarantine messaging and scheduler configuration; processor/messaging properties. |
| `client` | PSP authorization, capture, void and lookup clients. |
| `dto` | `PspAuthorizeRequestDto`, `PspAuthorizeResponseDto`, `PspCaptureRequestDto`, `PspVoidRequestDto`, `PaymentOperationRetryEnvelopeDto`, `PaymentOperationQuarantineEnvelopeDto`. |
| `command` | `PaymentOperationCommand`. |
| `model` | Operation context, PSP outcomes/mapping and scheduled retry data. |
| `exception` | Processing exceptions and poison failure codes. |
| `repository` | Operation processing, quarantine, scheduled retries and reconciliation persistence. |
| `messaging` | Kafka listeners, decoders, publishers, retry identity, worker pool, work admission and `QueuedCommand`. |
| `service` | Processor, attempt executors, retry/quarantine handoffs, scheduling/dispatch, reconciliation scanning, failure classification, backoff and processor identity. |
| `observability` | Blocked partition diagnostics and reconciliation scanner health. |

`QueuedCommand` stays in `messaging` because it holds Kafka execution state, including acknowledgment and assignment epoch. Retry and quarantine envelopes are DTOs because they represent transport payloads. Their Java names do not change Avro schemas.

`outbox` and `retention` retain their existing compact packages. Application-wide correlation context and HTTP correlation filtering remain in the root feature package `observability`.

## PSP simulator Maven project

Sources are under `psp-simulator/src/main/java/jhsoftware/pspsimulator`. Only `SimulatorApplication` stays at the application root.

| Package | Responsibility and examples |
| --- | --- |
| `controller` | `PspOperationController`, `SimulatorExceptionHandler`. |
| `service` | `PspOperationService`. |
| `repository` | `PspOperationRepository`. |
| `config` | `SimulatorConfiguration`. |
| `dto` | `AuthorizeOperationRequestDto`, `CaptureOperationRequestDto`, `VoidOperationRequestDto`, `OperationDecisionResponseDto`, `SimulatorApiErrorDto`. |
| `model` | `PspOperationRecord`, `AuthorizeOperationResult`, `CaptureOperationResult`, `VoidOperationResult`. |
| `fixture` | Authorization/capture/void outcome selectors and `PspFixtureOutcome`. |
| `exception` | Operation conflict/not-found and simulated fault exceptions. |

The three `*OperationResult` models contain a decision plus internal `stillProcessing` state used by the controller. They are not the HTTP response bodies and retain their names.

## Naming, visibility and tests

- Use `Dto` for transport contracts, `Command` for service/processing commands, and role-specific names for internal models. Do not add `Dto` merely because a type is a record.
- Place a new class with the responsibility it serves. Keep small, private client transport helpers near their client when appropriate.
- Make types and members public only when consumers in other packages require it. Package separation alone is not a reason to publish every constructor or helper.
- Keep focused tests under `src/test/java` in the same package as their target. For example, client tests belong in `payment.client`, and `ScheduledRetryDispatchTests` belongs in `processor.repository` because it directly tests `ScheduledRetryRepository`.
- Keep integration tests that span application concerns at a suitable common root. `PspOperationIntegrationTests` stays in `jhsoftware.pspsimulator`; the processor's three real messaging integration tests live in `processor.service` alongside their target services.
- Preserve existing test names so Maven includes and optional integration profiles continue to discover them.

Behavioral contracts remain in [contracts](../contracts/). Package refactors do not authorize changes to HTTP, PSP, Kafka, persistence or transaction behavior.
