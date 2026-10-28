#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

docker compose --profile messaging up -d --wait postgres kafka schema-registry

postgres_port="$(docker compose port postgres 5432 | sed 's/.*://')"

for consumer_group in payment-operation-command-listener paymentflow-retry-scheduler-v1; do
	docker compose exec -T kafka kafka-consumer-groups \
			--bootstrap-server localhost:29092 \
			--group "$consumer_group" \
			--reset-offsets --to-latest --all-topics --execute > /dev/null 2>&1 || true
done

./mvnw -q -f psp-simulator/pom.xml test package

SPRING_DATASOURCE_URL="jdbc:postgresql://localhost:${postgres_port}/paymentflow_psp_sim" \
SPRING_DATASOURCE_USERNAME=myuser \
SPRING_DATASOURCE_PASSWORD=secret \
SPRING_DOCKER_COMPOSE_ENABLED=false \
java -jar psp-simulator/target/psp-simulator-0.0.1-SNAPSHOT.jar > target/m6-psp-simulator.log 2>&1 &
psp_pid=$!
trap 'kill "$psp_pid" 2>/dev/null || true' EXIT

for attempt in $(seq 1 60); do
	if curl --silent --output /dev/null \
			http://localhost:8081/simulator/operations/00000000-0000-0000-0000-000000000000; then
		break
	fi
	if [[ "$attempt" == 60 ]]; then
		echo "PSP simulator did not become healthy; see target/m6-psp-simulator.log" >&2
		exit 1
	fi
	sleep 1
done

./mvnw -Pm6-failure-recovery "$@" test
