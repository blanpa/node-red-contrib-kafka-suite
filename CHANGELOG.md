# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.2] — 2026-04-27 (Beta)

### Fixed

- `kafkajs` backend: Snappy, LZ4 and ZSTD compression codecs are now
  registered automatically when their packages are installed. Previously
  the producer dropdown advertised LZ4/ZSTD/Snappy but neither the
  producer nor the consumer could actually handle them — consumers
  crashed with `KafkaJSNotImplemented: LZ4 compression not implemented`
  the moment they encountered a batch produced with one of those codecs,
  even when the consumer itself had compression set to `none`. Codec
  packages (`kafkajs-snappy`, `kafkajs-lz4`) are now declared as
  `optionalDependencies` so a fresh `npm install` pulls them in by
  default. ZSTD support is wired in but `@kafkajs/zstd` must be installed
  manually (its native build is fragile on newer Node versions; use the
  `confluent` backend for hassle-free ZSTD). If a codec package is
  missing, the producer now throws a clear error naming the package to
  install instead of failing inside kafkajs internals. Fixes #1.

## [0.0.1] — 2026-04-18 (Beta)

First public beta. Functionally complete and end-to-end tested against
multiple local Kafka distributions, but **not yet** validated against live
managed services with paid accounts.

### Added

- **Five Node-RED nodes**: `kafka-suite-broker` (config),
  `kafka-suite-producer`, `kafka-suite-consumer`, `kafka-suite-admin`, and
  `kafka-suite-schema-registry` (config).
- **Dual backend abstraction layer** (`lib/adapters/`): pluggable adapter
  interface with implementations for `kafkajs` and
  `@confluentinc/kafka-javascript`. Selectable per broker config node.
- **Service presets**: Confluent Cloud, AWS MSK (IAM + SCRAM), Azure Event
  Hubs, Aiven, Redpanda, self-hosted. The broker config node fills in the
  correct SSL/SASL shape automatically.
- **Authentication**: SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER,
  mutual TLS, plus unauthenticated.
- **Producer**: single + batch mode, message keys, headers, explicit
  partition selection, compression (GZIP, Snappy, LZ4, ZSTD), delivery
  confirmation. Dual outputs (success + error).
- **Consumer**: consumer groups, auto/manual commit, pause/resume via control
  input, JSON / UTF-8 / Buffer payload formats, configurable concurrency.
- **Admin**: `listTopics`, `createTopic`, `deleteTopic`, `describeCluster`,
  `listGroups`, `describeGroup`, `fetchTopicOffsets`, `resetOffsets`,
  `deleteGroup`. Dual outputs.
- **Schema Registry**: Confluent wire format for Avro, JSON Schema, and
  Protobuf. Optional auto-registration of schemas from the producer when
  `msg.schemaDefinition` is supplied.
- **Editor-side validation** for broker URLs, registry URL, topic names, and
  partition numbers.
- **MQTT-style shared connection** management: ref-counted client lifecycle,
  auto-reconnect with exponential backoff, status badges propagated to the
  Node-RED editor.
- **Test fixtures**: a `docker-compose.yml` (Confluent CP + Schema Registry +
  Node-RED), `docker-compose.redpanda.yml` (Redpanda PLAINTEXT), and
  `docker-compose.aiven-sim.yml` (mTLS Aiven-sim + SASL_SSL Redpanda).
- **`scripts/gen-test-certs.sh`** — generates self-signed CA + server +
  client PEM material for the mTLS test target.
- **`scripts/setup-redpanda-sasl.sh`** — bootstraps SCRAM users on the SASL
  Redpanda container.
- **Multi-broker E2E smoke test** (`test/integration/multi-broker-smoke.js`)
  — admin, single + batch produce, consume, and Avro Schema-Registry
  round-trip against four local clusters and both client backends.
- **163 unit tests** covering adapters, broker presets, HTML editor
  defaults, schema registry, and Node-RED node behaviour.

### Fixed (during beta hardening)

- `confluent-adapter`: SSL and SASL configuration was passed as a nested
  object inside the `kafkaJS` block, which the Confluent KafkaJS-compat
  client rejects with `"The 'ssl' property must be a boolean"`. PEM material
  and SASL credentials are now emitted as librdkafka root properties
  (`ssl.ca.pem`, `ssl.certificate.pem`, `ssl.key.pem`, `sasl.username`,
  `sasl.password`, `security.protocol`). This is what made mTLS and SASL_SSL
  actually work end-to-end with the Confluent backend.
- `confluent-adapter`: the connection-time reachability check called
  `admin.describeCluster()`, which is not exposed by the Confluent
  KafkaJS-compat admin API. Switched to the cheap-and-portable
  `admin.listTopics()`.
- `kafka-suite-consumer.html`: `inputLabels` was a string; Node-RED expects
  an array. Tooltip rendering is now correct.
- `kafka-suite-producer.html`: removed an empty/dead `oneditprepare`,
  added `inputLabels: ['payload']` for editor consistency.
- `kafka-suite-admin.html`: added `inputLabels: ['action']`.
- `kafka-suite-schema-registry`: the `autoRegister` checkbox was a dead
  switch — it is now wired into the producer, which calls `registerSchema()`
  before the first `encode()` when `msg.schemaDefinition` is supplied.

[0.0.2]: https://github.com/blanpa/node-red-contrib-kafka-suite/releases/tag/v0.0.2
[0.0.1]: https://github.com/blanpa/node-red-contrib-kafka-suite/releases/tag/v0.0.1
