# PaymentFlow

**PaymentFlow** is a learning-oriented, real-world-inspired **payment processing and orchestration platform**, designed around the reliability challenges of financial transactions and event-driven systems.

The project is being designed **system-design first**. Business requirements, invariants, failure scenarios, and architecture decisions are documented before implementing the Java services.

> **Status:** Design / requirements discovery. No payment processing capability has been implemented yet.

## Problem statement

An e-commerce application needs to initiate card payments through an external **Payment Service Provider (PSP)**. Network timeouts, retries, duplicated events, and partial failures must not lead to duplicate financial effects or lost transaction outcomes.

PaymentFlow will explore how to orchestrate this lifecycle while maintaining a traceable, recoverable source of truth.

## Intended scope

The initial case study focuses on **credit-card payments via a simulated PSP**, without handling real cardholder data or real money.

Planned capabilities to refine during requirements and system design:

- Accept payment requests and expose their current status.
- Authorize and capture payments through a simulated PSP.
- Receive PSP webhooks and recover from uncertain payment outcomes.
- Prevent duplicated payment creation and duplicated external financial effects.
- Explore asynchronous event processing, including Apache Kafka, with explicit failure-handling and delivery semantics.

Potential later extensions include reconciliation, a ledger, and notifications. **These are not commitments for the first implementation.**

## Engineering goals

- **Correctness:** model valid business transitions and financial invariants.
- **Idempotency:** make HTTP retries, consumer redelivery, and PSP retries safe.
- **Reliability:** recover from partial failures without silently losing payments.
- **Traceability:** correlate payment requests, operations, events, and PSP outcomes.
- **Pragmatic architecture:** introduce Kafka, services, and patterns when justified by requirements and measurable trade-offs.

Kafka is a transport/event-log technology, **not an automatic guarantee that an external payment will be executed exactly once**.

## Documentation

Documentation is the source of truth for decisions made during the system-design exercise.

### Agreed business requirements

| ID | Requirement |
| --- | --- |
| [BR-001](docs/requirement/BR-001-idempotent-payment-requests.md) | Repeated equivalent payment requests must not create a second financial operation. |
| [BR-002](docs/requirement/BR-002-uncertain-psp-outcomes.md) | An uncertain PSP outcome must not be treated as failure or blindly retried as a new operation. |
| [BR-003](docs/requirement/BR-003-expired-authorizations.md) | Confirmed expired authorizations cannot be captured. |
| [BR-004](docs/requirement/BR-004-idempotent-capture-requests.md) | Capture retries are idempotent and new captures after a completed capture are rejected. |
| [BR-005](docs/requirement/BR-005-reconciliation-after-24h-unknown.md) | An operation still UNKNOWN after 24 hours requires reconciliation, not automatic cancellation. |
| [BR-006](docs/requirement/BR-006-distinguish-technical-failure-from-decline.md) | Definitive technical failure, authorization decline, and uncertain outcome must remain distinct. |
| [BR-007](docs/requirement/BR-007-bounded-psp-retries.md) | Bounded, PSP-safe HTTP attempts with per-attempt timeouts of **5s → 10s → 20s**; exhaustion alone never proves failure. |
| [BR-008](docs/requirement/BR-008-block-conflicting-financial-operations.md) | Block new conflicting operations while prior PSP outcomes remain unresolved; no override in the MVP. |

### Non-functional requirements

- [NFR-001 — Transaction Traceability and Audit Retention](docs/requirement/NFR-001-transaction-traceability-and-retention.md) — durable payment audit trail; **5 years** financial/audit history, **30 days** technical logs, **7 days** Kafka messages.

### Domain design and decisions

- [Payment and PaymentOperation state machines](docs/domain/payment-state-machines.md) — approved business and operation states, including `REJECTED`, and their primary transitions.
- [Relational data model](docs/domain/relational-data-model.md) — four approved conceptual tables, fields, relationships, and cardinality, with implementation choices clearly identified.
- [ADR-0001 — Separate Payment from PaymentOperation](docs/adr/0001-separate-payment-and-payment-operations.md) — rationale, rejected alternatives, and consequences.
- [ADR-0002 — Append-only audit and retention](docs/adr/0002-append-only-audit-and-retention.md) — database audit history, rationale and retention boundaries.
- [ADR-0003 — Separate operation success from rejection](docs/adr/0003-distinguish-financial-success-from-rejection.md) — why `REJECTED` is distinct from `SUCCEEDED`, `FAILED`, and `UNKNOWN`.
- [ADR-0004 — Synchronous acceptance, asynchronous PSP processing](docs/adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md) — synchronous durable API acknowledgment, Transactional Outbox, Kafka delivery and asynchronous PSP execution.
- [ADR-0005 — Manual Kafka offset commit and recoverable processing](docs/adr/0005-manual-kafka-offset-commit-and-recoverable-processing.md) — commit consumption progress only after a durable outcome or recoverable handoff, never solely after an in-memory PSP result.
- [ADR-0006 — Consumer retries, poison-message quarantine and financial reconciliation](docs/adr/0006-distinguish-kafka-processing-retries-and-financial-reconciliation.md) — transient infrastructure failures use controlled retries, unprocessable Kafka messages use durable quarantine/DLQ, and uncertain PSP outcomes use reconciliation.
- [ADR-0007 — Single Kafka retry topic and DLQ](docs/adr/0007-single-kafka-retry-topic-and-dlq.md) — one retry destination and one quarantine destination for consumer failures; durable forwarding before source offset acknowledgment and DB-guarded financial operations.
- [ADR-0008 — Persistent Kafka Retry Scheduler](docs/adr/0008-persistent-kafka-retry-scheduler.md) — durably store retry eligibility and use a dispatcher instead of consumer pause/resume; PostgreSQL is the candidate schedule store.
- [ADR-0009 — Exponential backoff with jitter for Kafka retries](docs/adr/0009-exponential-backoff-with-jitter-for-kafka-retries.md) — consumer retry delays begin at 10 seconds, double with ±20% jitter and stop growing at a hard five-minute ceiling; no automatic DLQ on transient retry exhaustion.
- [ADR-0010 — Generic ExecutePaymentOperation command](docs/adr/0010-generic-execute-payment-operation-command.md) — one logical command for AUTHORIZE, CAPTURE and VOID, always referencing an existing persisted PaymentOperation; domain-event publication remains undecided.
- [ADR-0011 — ExecutePaymentOperation command envelope](docs/adr/0011-execute-payment-operation-command-envelope.md) — separates publication identity (`messageId`), financial identity (`operationId`) and trace identity (`correlationId`); defines command type, publication time and optional causation metadata (schemaVersion was subsequently removed in ADR-0016).
- [ADR-0012 — Full compatibility for Kafka command schema evolution](docs/adr/0012-full-compatibility-for-kafka-command-schema-evolution.md) — new and old consumers must understand supported compatible contract versions in both directions; breaking changes require an explicit migration while recoverable retry work remains readable.
- [ADR-0013 — Apache Avro + Schema Registry FULL_TRANSITIVE](docs/adr/0013-apache-avro-schema-registry-full-transitive.md) — schema-driven serialization for Kafka commands, with transitive backward/forward compatibility checks against registered versions.
- [ADR-0014 — RecordNameStrategy for Kafka Avro subjects](docs/adr/0014-record-name-strategy-for-kafka-avro-subjects.md) — Avro value-schema compatibility history belongs to each fully qualified record type, not to each Kafka topic; retry record schemas, if needed, remain separate.
- [ADR-0015 — Avro UUID, timestamp and optional field representations](docs/adr/0015-avro-logical-types-and-optional-command-fields.md) — UUID strings with Avro logical type, millisecond instants, nullable `causationId` with `default: null`, and no synthetic defaults for required identifiers.
- [ADR-0016 — Remove redundant command schemaVersion](docs/adr/0016-remove-redundant-command-schema-version.md) — omit `schemaVersion` from the Kafka command payload; structural schema identification belongs to Schema Registry.
- [ADR-0017 — Self-contained Avro command, optional Kafka headers](docs/adr/0017-avro-payload-as-command-source-kafka-headers-for-transport.md) — essential command identity and correlation fields stay in the Avro value; headers carry only optional transport and observability context.
- [ADR-0018 — paymentId as Kafka partition key](docs/adr/0018-payment-id-as-kafka-command-partition-key.md) — use the parent `paymentId` as the stable Kafka key for `ExecutePaymentOperation` commands to group a payment's operations within each topic; cross-topic retry ordering is not guaranteed.
- [ADR-0019 — Canonical UTF-8 UUID encoding for the Kafka key](docs/adr/0019-canonical-utf8-uuid-kafka-command-key.md) — serialize the `paymentId` key as lowercase hyphenated UUID text (36 UTF-8 bytes), consistently across command producers; the Avro value schema remains separate.
- [ADR-0020 — Bounded asynchronous workers with per-payment exclusion](docs/adr/0020-bounded-async-payment-processing-with-per-payment-exclusion.md) — process PSP commands off the Kafka poll thread with bounded concurrency, never run conflicting financial work for one payment simultaneously, and commit offsets only after durable outcomes or handoffs.
- [ADR-0021 — Small local MVP topology: 4 partitions / 1 consumer / 4 workers](docs/adr/0021-small-mvp-kafka-partition-and-worker-topology.md) — set a deliberately modest main-topic/Payment Processor baseline, preserving per-payment exclusion and safe Kafka offset commits. Retry/DLQ topology remains undecided.
- [ADR-0022 — Bounded per-payment waiting FIFO](docs/adr/0022-bounded-per-payment-waiting-queue.md) — commands received while their payment is busy may wait in a bounded local per-payment queue; waiting does not acknowledge Kafka, override DB eligibility, or count as a technical retry.
- [ADR-0023 — Single configured merchant for MVP](docs/adr/0023-configured-single-merchant-for-educational-mvp.md) — use a stable synthetic merchant UUID from application configuration; no unauthenticated client-provided merchant header.

Future system-design artifacts (functional/non-functional requirements, high-level architecture, event contracts, failure analysis, and further ADRs) will be added **as they are discussed and approved**.

## Implementation roadmap

- [PaymentFlow implementation milestones](docs/roadmap.md) — **M1 scope approved**; M2–M6 are proposed stages pending separate approval.
- [M1 — Payment Creation & Idempotency](docs/milestones/m1-payment-creation-idempotency.md) — initial REST API, PostgreSQL transactional persistence, idempotency, audit and **persisted but unpublished Outbox**. Kafka, PSP execution, workers and technical retries are intentionally outside M1. Merchant identity is a stable configured demo UUID (ADR-0023). Project bootstrap will be done by the user through Spring Initializr. API/DB/build contracts **and task-brief format** must be approved before Codex implementation.
- **Codex task conventions approved:** each task will be a concise, independently verifiable **vertical slice** in `docs/tasks/` with **Objective, Required Behavior, Scope Lock, Authoritative Sources, Acceptance Criteria and Dependencies**. Reference ADRs rather than duplicating them; the Architect chooses implementation details. **M1 task breakdown and open business/API/data contracts still require approval before task creation.**
- **Approved hybrid local development:** run Spring Boot from IntelliJ (IDE logs, breakpoints and DevTools), with PostgreSQL in Docker using Spring Initializr Docker Compose Support. [`compose.yaml`](compose.yaml) pins the PostgreSQL 17 major version and persists data in the named `postgres_data` volume. The user's starter baseline is Maven / Java 21 / Spring Boot 4.1.2 / JAR / YAML. An API Dockerfile and full Compose application stack are not approved yet.

## Proposed technical direction (not yet an implementation decision)

- **Language:** Java
- **Framework:** Spring Boot
- **Event platform to study:** Apache Kafka
- **Transactional data store candidate:** PostgreSQL
- **Local development candidate:** Docker Compose
- **Testing candidates:** JUnit and Testcontainers

Exact versions, service boundaries, event contracts, and persistence patterns remain open for evaluation.

## Safety boundary

All payment credentials, card tokens, and PSP interactions in this repository must be synthetic. No real PAN/CVV data, real payment credentials, or live financial transactions should be used in examples or tests.

## Current milestone

BR-001 through BR-008 and NFR-001 are documented. Payment/PaymentOperation state machines, append-only audit retention, bounded PSP HTTP attempt timeouts (5s/10s/20s), and blocking of conflicting unresolved operations are agreed. The conceptual four-table relational model and PSP outcome semantics are documented. **ADR-0004 approves synchronous durable request acceptance and asynchronous PSP execution via Transactional Outbox and Kafka.** ADR-0005 establishes manual offset commits after durable processing or independently recoverable handoff. ADR-0006 separates transient consumer retries, permanent-message quarantine and uncertain PSP reconciliation. ADR-0007 accepts one Kafka consumer retry topic and one DLQ topic, with durable handoff and database-enforced financial safety. ADR-0008 approves a persistent Retry Scheduler and durable due-work dispatch rather than consumer-local pause/resume. ADR-0009 approves exponential Kafka consumer retry backoff (10-second base, 2× growth, ±20% jitter, five-minute hard cap) without automatic DLQ for prolonged transient failures. ADR-0010 selects one generic `ExecutePaymentOperation` command for the existing AUTHORIZE/CAPTURE/VOID PaymentOperation types. ADR-0011 approves the conceptual command envelope, message/operation/correlation identity and optional causation; inclusion of `paymentId` in the Avro value remains undecided, while ADR-0018 later selects it as the Kafka key. ADR-0012 adopts Full Compatibility for additive Kafka command schema evolution, protecting older recoverable retry work. ADR-0013 selects Apache Avro with Schema Registry `FULL_TRANSITIVE` for Kafka commands. ADR-0014 approves Avro value subjects keyed by record type through `RecordNameStrategy`, keeping `FULL_TRANSITIVE` per record. ADR-0015 specifies Avro `uuid` logical types, `timestamp-millis` and nullable `causationId`. ADR-0016 removes the redundant command `schemaVersion` and relies on Schema Registry identity for structural versions. ADR-0017 keeps the complete essential command envelope in the Avro value and reserves Kafka headers for optional transport/observability data. ADR-0018 chooses stable `paymentId` as the Kafka record key for `ExecutePaymentOperation` publications, without changing the Avro value schema or guaranteeing cross-topic order. ADR-0019 standardizes the `paymentId` Kafka key as canonical lowercase UUID string in UTF-8, without an Avro key schema. ADR-0020 selects bounded asynchronous workers with per-payment exclusion for the deliberately modest educational MVP, with database financial guards and contiguous durable-before-commit offsets. Earlier 20/40 commands-per-second figures are hypothetical examples, not requirements. ADR-0021 sets the educational MVP's **main financial command topic to 4 partitions, 1 Kafka Consumer instance and 4 bounded asynchronous workers**; the retry/DLQ topology, queue bound and throughput targets remain separate/open. ADR-0022 approves a bounded local FIFO per payment for busy-key contention, not an automatic technical Kafka retry; numeric queue limits remain open. ADR-0023 chooses one stable configured merchant for the educational MVP, rather than a client-provided merchant header. Next, decide per-payment/global queue bounds and intake backpressure, then exact contiguous offset tracking, Scheduler/Dispatcher durable handoff, recovery and outstanding API/database details. The [implementation roadmap](docs/roadmap.md) now records the approved M1 scope and proposed later milestones; [M1 pre-coding contracts](docs/milestones/m1-payment-creation-idempotency.md) must be closed before Codex starts. The user will bootstrap with Spring Initializr and use Codex via [the `software-development-workflow` orchestration skill](https://github.com/Tartaminos/agent_benchmark/blob/remove-agents-from-flow/.agents/skills/software-development-workflow/SKILL.md) in `Tartaminos/agent_benchmark` at branch `remove-agents-from-flow`; the skill defines Scope Lock, an architect/coder/test-engineer pipeline (UI/UX only when applicable), independent gates and correction routing. The user confirms the Codex orchestration skill and agents are already configured locally; the agent repository is only a GitHub mirror. The six-section Markdown task format is approved; actual tasks await agreement on the M1 slice breakdown and required API/data contracts. No implementation code is authorized at this documentation-only stage.
