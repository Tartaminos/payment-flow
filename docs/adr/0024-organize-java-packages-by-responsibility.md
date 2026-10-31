# ADR-0024 — Java Packages by Responsibility and Explicit DTO Names

- **Status:** Accepted and implemented
- **Date:** 2026-10-31
- **Scope:** `payment`, `processor` and the `psp-simulator` Maven project
- **Related:** [Package guide](../architecture/package-organization.md), [ADR-0001](0001-separate-payment-and-payment-operations.md), [superseded REFACT-01 proposal](../tasks/REFACT-01-split-payment-capture-void-packages.md)

## Context

The flat `payment` and `processor` packages and the simulator root mixed HTTP contracts, services, persistence, configuration and integration concerns. A reader could identify the feature but could not readily locate controllers or DTOs. The user requested organization by responsibility and explicit DTO names across these areas.

The earlier REFACT-01 proposal extracted capture and void verticals while leaving controllers and DTOs in the `payment` root. That proposal was not implemented; the accepted organization below replaces it.

## Decision

1. Keep feature boundaries in PaymentFlow (`payment`, `processor`, `outbox`, `retention`) and subdivide the larger areas by responsibility. Do not introduce application-wide controller/service/repository packages spanning unrelated features.
2. Organize `payment` and `processor` using the responsibility packages listed in the [package guide](../architecture/package-organization.md). Organize simulator code under its existing `jhsoftware.pspsimulator` root using the same conventions where applicable.
3. Give HTTP transport objects and Java representations of Kafka retry/quarantine envelopes the suffix `Dto`, for example `CreatePaymentRequestDto`, `PspAuthorizeResponseDto` and `PaymentOperationRetryEnvelopeDto`. Keep `Request`/`Response` in names when they describe the transport direction.
4. Keep commands in `command` with the suffix `Command`. Internal models, execution state and service results retain their meaningful names; a Java record is not automatically a DTO. Examples include `PaymentOperationContext`, `ScheduledRetry`, `QueuedCommand` and `AuthorizeOperationResult`.
5. Keep the internal `CaptureValidityResponseDto` next to its only consuming client in `payment.client`. The suffix identifies its transport role without making it public or adding a shared abstraction.
6. Mirror the tested class's package for focused tests. Cross-cutting integration tests may remain at the application root; the simulator's HTTP/database/concurrency integration test stays there.
7. Widen visibility only for actual consumers across the new package boundaries. Preserve internal constructors, helpers and persistence details when no external consumer requires access. Keep application entry points at their roots for Spring component scanning and test discovery.
8. Leave the smaller `outbox` and `retention` packages unchanged. Reassess their organization when readability warrants subdivision; this decision does not require one-class packages everywhere.

## Compatibility and consequences

- This is a source organization change. It preserves endpoints, JSON fields, HTTP status codes, Avro record names/namespaces, topic and bean names, application properties, SQL, migrations, transaction semantics, retry rules and simulated fault behavior.
- Java DTO renames do not rename wire schemas. Existing Kafka `GenericRecord` conversion and Schema Registry contracts remain unchanged.
- Splitting the former package-private collaboration requires some public types and members. This increases the Java-visible surface; it does not introduce new external API endpoints.
- Existing dependency relationships are retained. In particular, `processor` still has collaboration between configuration, messaging, services and repositories; the package layout does not establish a newly enforced acyclic layering rule.
- The guide is the navigation reference for current placement. Existing milestone contracts remain authoritative for behavior, and historical planning records need not be rewritten to use current Java names.

## Alternatives

- Keep the flat packages: rejected because responsibilities remain difficult to locate.
- Split only by operation type (`capture`, `void_`): replaced because it does not make controllers, DTOs and persistence easy to find.
- Put every record in `dto`: rejected because internal models and commands have different roles.
- Apply identical package subdivisions to every small feature: deferred until there is a readability benefit.

## Validation recorded for the refactor

- PaymentFlow standard suite after package reorganization: 372 tests, zero failures/errors, 22 skipped by existing conditions.
- Processor Kafka integration tests executed explicitly: 13 tests, zero failures/errors/skips.
- Simulator suite after reorganization: 23 tests, zero failures/errors/skips.
- Final validity DTO rename: complete PaymentFlow main/test compilation and seven `CaptureAuthorizationValidityClientTests` passed.
- Independent normalized comparisons confirmed only packages, imports, required visibility and DTO identifiers changed; strings, SQL, resources and transport schemas were preserved. These results describe the refactor validations, not a new test run performed by this documentation update.
