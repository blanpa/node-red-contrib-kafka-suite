#!/usr/bin/env bash
# One-shot bootstrap for the redpanda-sasl test container: creates the
# `app` / `admin` SCRAM-SHA-256 users that the multi-broker smoke test expects.
# Idempotent: re-creating an existing user is a no-op.
set -e
CONTAINER="${1:-kafka-suite-redpanda-sasl}"

echo "[setup] waiting for $CONTAINER admin API"
for i in $(seq 1 30); do
  if docker exec "$CONTAINER" rpk cluster info --brokers "${CONTAINER}:29092" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[setup] creating SCRAM-SHA-256 users"
docker exec "$CONTAINER" rpk acl user create app   -p app-secret   --mechanism SCRAM-SHA-256 2>&1 | sed 's/^/  /' || true
docker exec "$CONTAINER" rpk acl user create admin -p admin-secret --mechanism SCRAM-SHA-256 2>&1 | sed 's/^/  /' || true

echo "[setup] DONE — users:"
docker exec "$CONTAINER" rpk acl user list 2>&1 | sed 's/^/  /'
