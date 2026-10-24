# M2-02 — Reliable Outbox Recovery

**Status:** PLANNED — backlog split approved; **NOT approved for Codex implementation** until this milestone and its contracts are approved.

## Objective

Make accepted Outbox work recoverable across broker outages, crashes and duplicate publication.

## Required Behavior

- Unconfirmed publication remains pending and retryable; progress is recorded only after a durable broker acknowledgment.
- Recover a stalled intent after restart without allocating a new financial operation or regenerating its original `messageId`.
- Allow at-least-once publication: a crash between broker acknowledgment and local progress update may produce duplicates.

## Scope Lock

**Include:** Publisher recovery, persistence state/claim coordination, and crash/failure integration tests.

**Exclude:** No exactly-once payment claim, no Kafka consumer offset management, no PSP execution or retry-scheduler implementation.

## Authoritative Sources

- [A4](../adr/0004-synchronous-acceptance-asynchronous-processing-with-outbox.md)
- [A11](../adr/0011-execute-payment-operation-command-envelope.md)
- [A17](../adr/0017-avro-payload-as-command-source-kafka-headers-for-transport.md)
- [DMODEL](../domain/relational-data-model.md)

## Acceptance Criteria

1. Broker unavailable: accepted payment/Outbox stays committed, pending and later publishable.
2. Crash after Kafka acknowledged but before marking sent: retry may duplicate the Kafka record but retains the same logical publication identity.
3. Publisher retry and concurrency do not lose or permanently strand pending intents.

## Dependencies

- [M2-01](M2-01-publish-first-outbox-command.md)
- M2 contract approval: claim/lease policy, broker durability acknowledgment, delivery state and publisher recovery/retention.
