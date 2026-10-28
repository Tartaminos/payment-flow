# M6 failure-recovery acceptance suite

The M6 acceptance entrypoint exercises PaymentFlow with real PostgreSQL containers,
the Compose Kafka broker and Schema Registry, and the stateful PSP simulator. All
identifiers and amounts are synthetic. The suite does not claim external exactly-once
effects, a recovery SLA, PCI compliance, or production high availability.

## Prerequisites

- Java 21
- Docker with the Compose plugin
- `curl`
- ports `8081`, `8082`, and `9092` available

Run the complete suite from the repository root:

```bash
./scripts/run-m6-failure-recovery.sh
```

The runner starts PostgreSQL, Kafka, and Schema Registry, builds and verifies the PSP
simulator, starts it on port `8081`, and runs the curated Maven profile
`m6-failure-recovery`. The application databases used by the acceptance tests are
isolated Testcontainers. The simulator log is written to
`target/m6-psp-simulator.log`.

Because the Compose broker keeps local topic data, the runner script itself advances
only the two PaymentFlow acceptance consumer groups to the current tail before the
suite; it does not delete topics or records. Use separate consumer groups for unrelated
local experiments that must preserve their own offsets. One test in the suite,
`M6EndToEndFailureRecoveryIntegrationTests.expiredKafkaAndTechnicalLogEvidenceCanBeRemovedWhilePostgresAuditReconstructsThePayment`,
does delete all records up to the live tail of the main topic across every partition as
part of exercising the Kafka-retention-expiry case; run this suite against a local
Compose stack dedicated to it, not one shared with unrelated consumers that still need
that backlog.

The runner stops its PSP process when it exits. To stop the supporting Compose services
without deleting their named volumes:

```bash
docker compose --profile messaging stop
```

## Deterministic failure matrix

| Contract case | Automated evidence in the M6 profile |
| --- | --- |
| Dropped client response after commit | `M6EndToEndFailureRecoveryIntegrationTests.droppedClientResponseReplayRecoversOneAuthorizationAcrossTheCompleteWorkflow` and `PaymentAdmissionIntegrationTests` |
| Broker ack followed by lost Outbox update | `OutboxPublisherTests.brokerAcknowledgementFollowedByDatabaseFailureKeepsTheOriginalMessageRetryable`, duplicate handling in `PaymentOperationProcessingRepositoryTests` |
| Broker/Registry publication outage | `OutboxRealMessagingIntegrationTests.realBrokerOutageLeavesOutboxPendingAndTheSameMessageRecoversWhenBrokerReturns` uses an actually unreachable producer, durable PostgreSQL Outbox state, then the real broker/Registry for recovery |
| Redelivery, out-of-order completion, crash/rebalance | `M6EndToEndFailureRecoveryIntegrationTests.realConsumerRestartRedeliversDuplicateWithoutRepeatingTheFinancialEffectOrSkippingItsOffset` restarts the real listener, redelivers through Kafka and checks the committed group offset; worker ordering/fencing variants remain in `PaymentOperationCommandListenerTests`, `PaymentOperationRebalanceListenerTests`, and `PaymentWorkAdmissionTests` |
| PSP persisted result with dropped reply | `M6EndToEndFailureRecoveryIntegrationTests.persistedPspEffectWithDroppedReplyReusesTheOriginalFinancialIdentity`; the simulator fixture is verified by `PspOperationIntegrationTests` |
| Capture/void/expiry races and paths | `M6EndToEndFailureRecoveryIntegrationTests.captureVoidAndExpiryUseTheRealKafkaAndStatefulPspBoundaries` drives API → Kafka → stateful PSP → PostgreSQL and races capture against void; focused admission/repository tests retain boundary coverage |
| Retry handoff, storage failure/restart, duplicate dispatch | the three `*RealMessagingIntegrationTests` for handoff, scheduling, and dispatch |
| Poison Avro quarantine before offset eligibility | `PaymentOperationTechnicalRetryHandoffRealMessagingIntegrationTests` and `PaymentOperationRetrySchedulerRealMessagingIntegrationTests` |
| UNKNOWN at 24h and late authoritative resolution | `UnknownOperationReconciliationScannerTests`, `ReconciliationEscalationRepositoryTests`, and `ReconciliationJobRepositoryTests` |
| Audit/retention reconstruction and privacy | `M6EndToEndFailureRecoveryIntegrationTests.expiredKafkaAndTechnicalLogEvidenceCanBeRemovedWhilePostgresAuditReconstructsThePayment` deletes real main-topic records and a technical log file, then reconstructs state/audit from PostgreSQL; retention holds and seven-day configuration are verified separately |

An ambiguous unprocessed PSP operation is also driven through the real stack and asserted
to remain `UNKNOWN`, with the original operation identity, three bounded attempts, an
independently durable reconciliation job, and no fabricated failed/declined payment.
