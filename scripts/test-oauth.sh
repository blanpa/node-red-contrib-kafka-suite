#!/usr/bin/env bash
# End-to-end OAUTHBEARER (OAuth 2.0 / OIDC) integration test.
#
# Brings up Keycloak (OIDC IdP) + Redpanda (native OIDC OAUTHBEARER validation),
# then runs test/integration/oauth-smoke.js from INSIDE the compose network so
# it can reach both services by hostname (no reliance on host port publishing).
#
# Usage:
#   ./scripts/test-oauth.sh                 # kafkajs backend
#   BACKENDS=kafkajs,confluent ./scripts/test-oauth.sh
#   KEEP_UP=1 ./scripts/test-oauth.sh       # leave the stack running afterwards
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker-compose.oauth.yml"
BACKENDS="${BACKENDS:-kafkajs}"

cleanup() {
  if [ "${KEEP_UP:-0}" != "1" ]; then
    echo "[oauth-test] tearing down stack"
    $COMPOSE down >/dev/null 2>&1 || true
  else
    echo "[oauth-test] KEEP_UP=1 — leaving stack running ($COMPOSE down to stop)"
  fi
}
trap cleanup EXIT

echo "[oauth-test] starting Keycloak + Redpanda (this can take ~90s on first run)"
$COMPOSE up -d keycloak redpanda-oauth

echo "[oauth-test] waiting for Redpanda to report healthy"
for i in $(seq 1 40); do
  h=$(docker inspect kafka-suite-redpanda-oauth --format '{{.State.Health.Status}}' 2>/dev/null || echo starting)
  [ "$h" = "healthy" ] && break
  sleep 3
done
[ "$h" = "healthy" ] || { echo "[oauth-test] Redpanda did not become healthy"; exit 1; }

echo "[oauth-test] running smoke test (backends: $BACKENDS)"
$COMPOSE run --rm -e "BACKENDS=$BACKENDS" oauth-tester
