# M2-01 — First Outbox Command Published

**Status:** APPROVED for implementation (2026-10-24), **after M1-02 is complete and verified**. Execute through the locally configured Codex orchestration skill; approval does not launch Codex.

## Objective

Publish a previously committed `ExecutePaymentOperation` Outbox intent to Kafka in the local development environment.

## Required Behavior

- Run a local Kafka broker and Schema Registry without changing the IntelliJ-hosted Spring Boot/PostgreSQL workflow.
- Deliver a decodable Avro command with the accepted message identity/envelope and Schema Registry `RecordNameStrategy` / `FULL_TRANSITIVE` contract; do not add `schemaVersion`.
- Use the parent `paymentId` as the Kafka key, canonical lowercase UUID UTF-8 (36 bytes); broker receipt does not mean financial authorization.

## Scope Lock

**Include:** Outbox-to-Kafka publication path, Avro schema/registry contract and automated integration checks.

**Exclude:** No PSP calls, financial state transitions, consumer workers, technical retry topics or guaranteed exactly-once delivery.

## Authoritative Sources

- [M2 approved implementation contract](../contracts/m2-outbox-kafka-contract.md) (**APPROVED**)
- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [A10](../adr/0010-generic-execute-payment-operation-command.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)
- [A12](../adr/0012-full-compatibility-for-kafka-command-schema-evolution.md)
- [A13](../adr/0013-apache-avro-schema-registry-full-transitive.md)
- [A14](../adr/0014-record-name-strategy-for-kafka-avro-subjects.md)
- [A15](../adr/0015-avro-logical-types-and-optional-command-fields.md)
- [A16](../adr/0016-remove-redundant-command-schema-version.md)
- [A17](../adr/0017-avro-payload-as-command-source-kafka-headers-for-transport.md)
- [A18](../adr/0018-payment-id-as-kafka-command-partition-key.md)
- [A19](../adr/0019-canonical-utf8-uuid-kafka-command-key.md)

## Acceptance Criteria

1. A committed pending M1 intent produces **at least one** decodable Kafka command with matching `messageId`, `operationId`, `correlationId` and `paymentId` key; M2 does **not** assert exactly-once publication.
2. Schema compatibility checks enforce `FULL_TRANSITIVE` and the correct record-name subject; a nullable `causationId` is supported.
3. Incorrect/missing operation identity or key is not silently replaced by random data, and no PSP action occurs.

## Dependencies

- [M1-02](M1-02-idempotent-replay-and-concurrent-requests.md)
- [Approved M2 transport and wire contract](../contracts/m2-outbox-kafka-contract.md): local Kafka/Schema Registry, topic/retention, record name, key-only `paymentId`, serialization and registry identity.
