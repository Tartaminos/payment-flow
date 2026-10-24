# PaymentFlow — Implementation Roadmap

- **Status:** Roadmap established; **all six milestones, six contracts and 21 tasks APPROVED for sequential Codex implementation** (2026-10-24). No implementation is implied by planning approval.
- **Date:** 2026-10-24
- **Source of truth:** This file, the [M1 specification](milestones/m1-payment-creation-idempotency.md), [business requirements](requirement/), [domain](domain/) and [accepted ADRs](adr/).
- **Method:** Deliver small, reviewable increments. Codex implements only a milestone after its contracts, acceptance criteria **and task-brief structure** are approved and committed. No downstream design gap authorizes inventing financial business rules.
- **Approved task-granularity rule (2026-10-24):** implementation tasks must be **vertical slices** organized around one independently verifiable behavior/capability, spanning any necessary controller/domain/persistence/test changes. Do not split tasks by horizontal layers (controllers/services/repositories), create arbitrary class-by-class tasks, or bundle an entire milestone into one overly broad task.
- **Approved task source and location (2026-10-24):** each vertical slice gets **one short Markdown file under `docs/tasks/`**, versioned on GitHub and selected as input to the Codex orchestration skill. The **six-section template is approved**. [Two M1 slices](milestones/m1-payment-creation-idempotency.md) have now been created as **APPROVED**, together with the [detailed contract](contracts/m1-payment-creation-contract.md).

## Approved concise task format (2026-10-24)

Every task in `docs/tasks/` is a **small vertical slice**, written in concise Markdown using these sections in this order:

1. **Objective** — the one capability/outcome delivered.
2. **Required Behavior** — observable success and relevant failure semantics, not Java implementation steps.
3. **Scope Lock** — explicit inclusions and exclusions.
4. **Authoritative Sources** — links to the exact requirements, contracts, relevant ADRs and milestone.
5. **Acceptance Criteria** — few, measurable, independently testable checks, including significant negative/concurrency paths when relevant.
6. **Dependencies** — prerequisite approved tasks/contracts, or `None`.

Prefer pointers over repeated documentation. Do **not** prescribe class/controller/service/repository layouts: the orchestration skill's Architect owns architectural placement, the Coder implements, and the Test Engineer provides independent automated evidence. The task defines *what* must be true, not a technical recipe.

This policy approves the **format**, not the M1 decomposition. Both M1 tasks and the implementation contract were approved on 2026-10-24 and are ready for user-initiated Codex execution.

## Complete task backlog (approved decomposition)

The [21-task backlog index](planning/proposed-complete-task-backlog.md) links all **21 six-section Markdown task files** under `docs/tasks/`. The user **approved the 21-task division** on 2026-10-24. **All 21 tasks are APPROVED for implementation, gated by completion and independent verification of their Dependencies**. Approval does not mean tasks were executed. No runtime work is implied by task-file creation. See the [task review report](planning/task-backlog-review.md) for consistency checks and future contract gates.

## M2 approved transport and Publisher contract

The [M2 Outbox/Kafka/Avro transport contract](contracts/m2-outbox-kafka-contract.md) is **APPROVED (2026-10-24)**. It defines a local KRaft broker + Schema Registry, the 4-partition/7-day main topic, the Avro record and **key-only `paymentId`**, and at-least-once lease/ack-based Publisher recovery. **Execution order:** finish/verify M1-01 → M1-02, then **[M2-01](tasks/M2-01-publish-first-outbox-command.md) → [M2-02](tasks/M2-02-recover-outbox-publication.md)**. Approval does not start Codex or deploy Kafka.

## Approved downstream contracts (M3–M6)

The user explicitly approved the four executable contracts on **2026-10-24**: [M3 — PSP simulator, active UNKNOWN recovery and Kafka offsets](contracts/m3-payment-processor-contract.md), [M4 — capture/void and authoritative expiry](contracts/m4-capture-void-expiration-contract.md), [M5 — technical retry, DLQ and persistent Scheduler](contracts/m5-technical-retry-dlq-scheduler-contract.md) and [M6 — reconciliation, traceability and end-to-end verification](contracts/m6-reconciliation-observability-acceptance-contract.md). **All 17 M3–M6 task files are APPROVED**, in addition to M1 and M2. See the [approved decision review](planning/m3-m6-contract-decision-review.md).

**Execution remains gated by task dependencies and independent specialist/test reviews; approval does not run Codex or start services.** M3 creates active independent AUTHORIZE UNKNOWN recovery, M4 extends it to CAPTURE/VOID before acknowledging their offsets, and M6 completes reconciliation/operability.

| ID | Delivery | Status | Minimal demonstrable outcome |
| --- | --- | --- | --- |
| **M1** | **Payment Creation & Idempotency** | **APPROVED for Codex** | POST /payments persists Payment, initial AUTHORIZE PaymentOperation, idempotency association, audit and an Outbox publication intent in one transaction; returns durable acceptance, with tests. **No Kafka/PSP integration.** |
| **M2** | Outbox Publisher & Kafka Transport | **APPROVED for Codex after M1** | Committed Outbox intents are published at least once to the main Kafka command topic with agreed paymentId key and Avro/Schema Registry value. |
| **M3** | Payment Processor & Simulated Authorization | **APPROVED for Codex after prerequisites** | One consumer decodes commands; bounded workers check DB state, call a fake PSP for AUTHORIZE, store definitive/unknown results and commit safe offsets. |
| **M4** | Capture, Void & Domain Invariants | **APPROVED for Codex after prerequisites** | Admitted capture/cancel operations respect authorization, expiry, single-full-capture and conflicting-operation rules. |
| **M5** | Technical Retry, DLQ & Durable Scheduling | **APPROVED for Codex after prerequisites** | Consumer technical errors have recoverable handoff to the single retry topic/DLQ; a persistent Retry Scheduler/Dispatcher redrives eligible work safely. |
| **M6** | PSP Uncertainty, Reconciliation & Observability | **APPROVED for Codex after prerequisites** | Ambiguous outcomes are independently reconciled/escalated, and tests/diagnostics exercise failure recovery and tracing. |

The overall sequence is an **approved implementation plan**; individual tasks start only after prerequisite tasks pass their Codex verification gates. PSP webhooks, advanced operational tooling and any ledger remain outside approved scope.

## M1 approved execution slices

- **[M1-01 — Transactional Payment Acceptance](tasks/M1-01-transactional-payment-acceptance.md)**: new POST /payments creates all five durable records atomically. **Approved for sequential implementation.**
- **[M1-02 — Idempotent Payment Replay and Concurrency](tasks/M1-02-idempotent-replay-and-concurrent-requests.md)**: the same endpoint handles equivalent replay, conflicting keys and concurrent first requests, with PostgreSQL-backed evidence. Depends on M1-01. **Approved for sequential implementation.**
- **[M1 accepted HTTP, idempotency and persistence contract](contracts/m1-payment-creation-contract.md)**: accepted 202/400/409 responses, canonical SHA-256, no-expiry M1 policy, Outbox persistence and Testcontainers tests.

## Approved M1 boundaries

The user explicitly approved **API + PostgreSQL + transactional Outbox + audit + automated tests, without Kafka in the first increment**. The approved implementation contract and tasks are tracked in [M1](milestones/m1-payment-creation-idempotency.md).

The API accepts requests durably; it does **not** report financial authorization/capture success before PSP processing exists. The Outbox is **persisted but not published** in M1.

**Initializr dependency decision (2026-10-24):** include **Spring for Apache Kafka** and **HTTP Client** in the starter generated by the user from the beginning. Milestone boundaries govern behavior/runtime integration, not whether future libraries already exist in the build. M1 must work without running Kafka and must not perform PSP requests. This approves only those two dependencies; no other proposed starter selections are implicitly accepted.

**Additional agreed implementation context:** one stable synthetic merchant from application configuration ([ADR-0023](adr/0023-configured-single-merchant-for-educational-mvp.md)); the user will bootstrap the project through **Spring Initializr**. For subsequent Codex work, the intended tool is the **orchestration skill in the `remove-agents-from-flow` branch of the `Tartaminos/agent_benchmark` repository**. The repository, branch and skill path were verified through the connected GitHub repository: [`software-development-workflow/SKILL.md`](https://github.com/Tartaminos/agent_benchmark/blob/remove-agents-from-flow/.agents/skills/software-development-workflow/SKILL.md). The skill requires Scope Lock, specialist gates and independent automated tests; commits and PRs require explicit authorization. The user confirms the Codex skill and its agents are already configured **locally** for use in the PaymentFlow project; the GitHub agent repository is a mirror. No additional skill setup task is needed. Vertical-slice task granularity and `docs/tasks/*.md` storage are now approved. The task structure is now approved: **Objective, Required Behavior, Scope Lock, Authoritative Sources, Acceptance Criteria and Dependencies**. Keep the file concise; refer to source docs instead of repeating ADRs and let the skill's Architect choose technical implementation details. The **M1 slice breakdown and API/data contract are approved**.

## Suggested delivery rhythm

1. Agree on a milestone's scope.
2. Close only the contracts required for that milestone in GitHub (ADRs/specs where needed).
3. **Use the approved concise six-section Markdown template** below for each vertical slice, linking rather than reproducing applicable requirements and ADRs. The [approved M1 contract](contracts/m1-payment-creation-contract.md) and **two approved vertical-slice tasks** are committed. Execute with the local orchestration skill, one slice at a time.
4. The user initializes the baseline via Spring Initializr; Codex implements approved small tasks through the selected orchestration workflow. Run tests and review contributions against the GitHub source-of-truth docs.
5. Update documentation for explicitly approved changes; move to the next milestone after verification.

**Bootstrap and containerization:** the user will generate the Spring Boot starter through Spring Initializr. **Approved for M1 local development (2026-10-24): Spring Boot runs from IntelliJ on the host**, with IDE console/debugger/DevTools; **PostgreSQL runs in a Docker container** managed via Docker Compose Support; [`compose.yaml`](../compose.yaml) now uses the `postgres:17` image and named `postgres_data` volume at `/var/lib/postgresql/data`. The user's Initializr selections include Maven, Java 21, Spring Boot 4.1.2, JAR and YAML. A Dockerfile for the API and complete application-in-containers mode remain open scope choices. Spring Boot's development-time Docker Compose integration does not itself dockerize the Java application.

**Avoid speculative scaling:** local educational MVP, no production traffic target. When Kafka workers are implemented, the approved initial main-topic baseline is 4 partitions / 1 consumer instance / 4 async workers (ADR-0021). Backpressure bounds and detailed recovery contracts are **approved for M3–M6**; see the linked milestone contracts.

## Design references

- [ADR-0004](adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md): single DB transaction for admission + Outbox.
- [BR-001](requirement/BR-001-idempotent-payment-requests.md): replay-safe payment creation.
- [Conceptual relational data model](domain/relational-data-model.md): four core domain tables; Outbox is additional supporting persistence.
- [ADR-0010](adr/0010-generic-execute-payment-operation-command.md): generic command for an already persisted PaymentOperation.
- [ADR-0020](adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md), [ADR-0021](adr/0021-small-mvp-kafka-partition-and-worker-topology.md), [ADR-0022](adr/0022-bounded-per-payment-waiting-queue.md): future worker/concurrency design.
