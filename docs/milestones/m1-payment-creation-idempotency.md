# M1 — Payment Creation & Idempotency

- **Status:** Scope approved on 2026-10-24. **Implementation not yet authorized until the pre-coding contracts below are approved and committed.**
- **Approved task design:** when we later prepare tasks for Codex, each must be a **vertical slice** with independently verifiable behavior, recorded in its own Markdown file under `docs/tasks/`. The exact internal task template, sequencing and tests presentation remain to be decided; **no tasks have been created yet**.
- **Roadmap:** [PaymentFlow milestones](../roadmap.md)
- **Relevant source:** [BR-001](../requirement/BR-001-idempotent-payment-requests.md), [BR-008](../requirement/BR-008-block-conflicting-financial-operations.md), [ADR-0001](../adr/0001-separate-payment-and-payment-operations.md), [ADR-0002](../adr/0002-append-only-audit-and-retention.md), [ADR-0004](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md), [ADR-0010](../adr/0010-generic-execute-payment-operation-command.md), [ADR-0023 — Configured merchant](../adr/0023-configured-single-merchant-for-educational-mvp.md), [Domain model](../domain/relational-data-model.md)

## Objective

Build the first runnable **local** vertical slice: accept an idempotent payment request, validate it and durably register all required records in one PostgreSQL transaction. The outbox record represents the **intention** to later execute the newly persisted authorization operation. **No actual external financial effect is performed in M1.**

## Approved deliverable boundaries

1. An HTTP **POST /payments** accepting the already discussed `Idempotency-Key`. **Merchant identity is resolved from a stable synthetic UUID in application configuration** per ADR-0023, not from `X-Merchant-Id` or an HTTP body field. The exact JSON contract remains open.
2. Request validation using the accepted domain basics: amount in **integer minor units**, currency and external order association. Reject invalid input without persisting partial payment artifacts.
3. Atomically persist:
   - `Payment` with initial `CREATED` state.
   - Associated `PaymentOperation` of type `AUTHORIZE` with initial `PENDING` state.
   - Client idempotency association scoped to the request and existing operation.
   - Append-only audit event for the admission/creation transition.
   - **Durable Outbox publication intent** corresponding to the existing PaymentOperation; command type will be `ExecutePaymentOperation` when subsequently published.
4. A success acknowledgment only **after the transaction commits**. For this first slice the agreed target is **HTTP 202 Accepted**, identifying the Payment, and explicitly **not** reporting PSP success.
5. Unit/integration tests demonstrating idempotency, concurrency and transaction atomicity.

**Technical direction before Codex:** Maven, Java 21, Spring Boot 4.1.2, JAR packaging, YAML configuration and PostgreSQL are the user's selected Spring Initializr baseline (2026-10-24). The user generates the starter; migration design, remaining dependency choices and test tooling still require agreement. Initializr generation is performed by the user, not an unrequested Codex scaffold.

**Initializr dependency choice approved:** include **HTTP Client** and **Spring for Apache Kafka** in the generated project's dependencies now, because both are already planned for later milestones. Their inclusion in the build is **not** authorization to implement PSP HTTP calls, Kafka producers/consumers, topic creation or broker-dependent startup in M1. Keep M1 runnable with PostgreSQL but without an active Kafka broker. Other dependency/configuration selections still require separate approval.

## Initial testable acceptance criteria

- **M1-AC-01 — New request:** one valid request yields **one** Payment in `CREATED`, **one** `AUTHORIZE:PENDING` operation, one scoped idempotency association, an admission audit entry and one pending Outbox intent; the request receives durable acceptance.
- **M1-AC-02 — Equivalent replay:** the same client/merchant/operation scope, idempotency key and equivalent business request resolve to the **original payment/operation** without duplicate Outbox intent, audit admission or operation.
- **M1-AC-03 — Conflicting replay:** same scoped key but different business parameters produces a documented conflict without modifying or duplicating the original payment. HTTP `409` is the existing candidate from BR-001 and requires contract confirmation.
- **M1-AC-04 — Concurrent replay:** simultaneous equivalent requests cannot produce two payments or two independent AUTHORIZE operations; safe responses for the loser follow the agreed API contract.
- **M1-AC-05 — Atomic rollback:** a controlled persistence failure before transaction completion leaves **none** of the new Payment, PaymentOperation, idempotency, audit or Outbox entries committed, and does not report successful acceptance.
- **M1-AC-06 — No financial side effect:** no Kafka publication, no consumer, no worker processing and no PSP HTTP request occur in this milestone.
- **M1-AC-07 — Restart durability:** persisted accepted work and its pending Outbox intent remain inspectable after restarting the application; no in-memory queue is the system of record.
- **M1-AC-08 — Safety:** fake identifiers/data only; no real PAN, CVV or PSP secrets.

## Not in M1

- Kafka broker/client/topic creation, Avro physical schemas, Schema Registry, Outbox Publisher runtime, Kafka offsets and retries.
- PSP simulator execution, `AUTHORIZED`, `CAPTURED`, `DECLINED` financial results, capture/void endpoints, reconciliation/webhooks.
- Four worker execution slots, backpressure, local payment queues and Consumer Group mechanics; those remain future milestones.
- Production capacity/SLA, deployment to cloud, ledger, notification infrastructure.

## Contracts to close **before asking Codex to implement**

1. **API contract:** request/response payload for `POST /payments`, validation and error envelopes, replay/concurrent-race HTTP response semantics, final confirmation of proposed `202`/`409`. **Merchant identity source is already decided** in ADR-0023.
2. **Idempotency rules:** canonical request hashing, precise merchant + operation + key scope, race resolution/unique constraints and minimum retention behavior. The conceptual model specifies the scope but not all physical rules.
3. **Physical persistence:** PostgreSQL migration design for the four accepted core entities **plus an Outbox supporting table**; transaction boundaries, outbox identity/publication state, and minimal future key/schema references. No Kafka publisher yet.
4. **Build/testing baseline:** user boots the project via Spring Initializr; decide Java/Spring Boot/dependency versions, build tool, local PostgreSQL setup and automated integration-test strategy. **Approved local development mode (2026-10-24):** the Spring Boot application runs as a local process in **IntelliJ** with the native IDE debugger, logs and DevTools; **PostgreSQL runs in Docker** with **Docker Compose Support** in the Initializr project. The repository's [`compose.yaml`](../../compose.yaml) now uses `postgres:17` and the named `postgres_data` volume for PostgreSQL persistence. A Dockerfile for the API and complete application-in-containers mode remain separate open decisions, not M1 requirements.
5. **Task/orchestration format gate:** before creating any tasks/briefs, inspect the exact orchestration skill in `Tartaminos/agent_benchmark` / branch `remove-agents-from-flow`, discuss how tasks are grouped, represented, reviewed and sequenced, and explicitly approve that structure. The verified path is `Tartaminos/agent_benchmark/.agents/skills/software-development-workflow/SKILL.md` on `remove-agents-from-flow`. The skill uses Scope Lock plus architect, coder and test-engineer gates for backend implementation. The user already has the skill configured locally. Task granularity (vertical slices) and location (`docs/tasks/*.md`) are approved; the **Markdown template, dependency/sequencing convention and validation presentation** are not yet decided. **Do not generate tasks or briefs yet.**
6. **Codex execution artifact:** only after gate 5, create approved task(s) with precise scope and executable acceptance tests; commit them before coding.

## Definition of Done

The API starts locally, migrations run, tests pass, persistence can be inspected, and all eight acceptance criteria are demonstrated. Review checks for consistency with accepted BRs and ADRs and verifies **no Kafka or PSP execution was introduced**.

The milestone is **approved in scope**, **not yet implementation-ready** until the contracts above are decided. Codex must not invent missing payment business rules.
