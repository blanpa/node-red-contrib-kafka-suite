#!/usr/bin/env bash
# Generate a self-signed CA + server cert + client cert for the Aiven mTLS
# simulation. The output mimics what Aiven hands you when you click
# "Authentication > Download CA certificate / client cert / client key".
#
# Usage:  ./scripts/gen-test-certs.sh
# Output: ./test/certs/{ca,server,client}.{pem,key,crt}
#
# All material is for local testing ONLY. Never deploy these.

set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)/test/certs"
mkdir -p "$DIR"
cd "$DIR"

DAYS=3650

if [[ -f ca.pem && -f server.crt && -f client.key.pem ]]; then
  echo "[certs] already exist in $DIR — skipping. Delete the directory to regenerate."
  exit 0
fi

echo "[certs] generating CA"
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days $DAYS \
  -subj "/CN=kafka-suite-test-ca/O=node-red-contrib-kafka-suite/L=local" \
  -out ca.pem

echo "[certs] generating server cert (CN=aiven-sim, SAN=localhost,aiven-sim)"
openssl genrsa -out server.key 2048 2>/dev/null
cat > server.cnf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = aiven-sim
O  = kafka-suite-test
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
DNS.2 = aiven-sim
DNS.3 = kafka-suite-aiven-sim
IP.1  = 127.0.0.1
EOF
openssl req -new -key server.key -out server.csr -config server.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.crt -days $DAYS -sha256 -extensions v3_req -extfile server.cnf

echo "[certs] generating client cert (CN=kafka-suite-client)"
openssl genrsa -out client.key.pem 2048 2>/dev/null
openssl req -new -key client.key.pem -out client.csr \
  -subj "/CN=kafka-suite-client/O=kafka-suite-test"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.crt -days $DAYS -sha256

echo "[certs] cleanup intermediate files"
rm -f server.csr client.csr server.cnf ca.srl

# Make readable by Redpanda container's redpanda uid (101)
chmod 644 *.pem *.crt *.key 2>/dev/null || true

echo "[certs] DONE — files in $DIR"
ls -la "$DIR"
