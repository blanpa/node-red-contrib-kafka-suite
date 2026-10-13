# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.6] — 2026-10-13

### Fixed

- **`PROTOCOL://host:port` broker addresses now work.** The runtime accepted
  them but passed them unchanged to the client, which split on `:` and tried
  to connect to host `SASL_SSL`. The prefix is now stripped, and the editor
  validation accepts the same forms as the runtime.
- **SASL without TLS (`SASL_PLAINTEXT`) is configurable.** Every SASL
  mechanism forced TLS on. New **Use TLS** checkbox (default on, so existing
  configs are unchanged); a custom CA cert can now be set for SASL+TLS too.
- **Dead consumers are detected.** The broker listened for a `disconnected`
  event that only fired on our own shutdown, so a lost connection was never
  noticed. kafkajs consumer crashes (`restart: false`) are now forwarded and
  the consumer is rebuilt with backoff instead of showing "consuming" while
  stopped.
- **Confluent backend:** consumer concurrency (`partitionsConsumedConcurrently`)
  was ignored; retry settings and log level were not mapped; consumer
  `rebalancing`/`ready` status now works via librdkafka's rebalance callback;
  the producer connects at startup so connection errors surface immediately.
- **`0` values are kept.** `retries: 0` became 5 and `msg.key = 0` became
  `null` because of `parseInt(x) || default` / `||` fallbacks.
- **Binary keys** (Buffer) are no longer stringified by either adapter.

### Changed

- **No more 500 ms polling and log flooding.** Child nodes start when the
  broker reports a connection instead of polling every 500 ms, and failed
  setups (missing topic, ACL denied, ...) retry with exponential backoff —
  previously each failure logged an error twice per second per node. Broker
  reconnects use real exponential backoff (`Init Retry` doubling up to
  `Max Retry`) instead of a fixed 300 ms.
- **Schema auto-registration is cached** per subject/type/definition, so
  `msg.schemaDefinition` on every message no longer costs an HTTP round-trip.
  The producer encodes with the id returned by the registration.
- **Admin: destructive actions need opt-in.** `deleteTopic`, `deleteGroup` and
  `resetOffsets` require the new **Allow destructive actions** option. New
  nodes start with it off; existing nodes keep their behavior. A fixed
  **Action** can be chosen in the node config.
- **Admin: `createTopic` defaults to the broker's partition count and
  replication factor** (`-1`) instead of `1`/`1`, which failed on managed
  clusters requiring RF 3. Explicit values are still honored.

### Added

- Producer **Headers from** / consumer **Headers to**: configurable `msg`
  property for Kafka headers (default `headers`), to avoid clashing with the
  HTTP nodes' `msg.headers`. Producer header values are coerced to valid
  Kafka header types.
- Consumer **commit via input**: send a processed message back with
  `msg.action = "commit"`. Works where `msg.commit()` (a function) is lost.
- Consumer **Key format** (`string` / `raw`), **Session Timeout** and
  **Heartbeat** settings. A pause now survives a consumer restart.
- Producer **Auto-create** option (default on) for `allowAutoTopicCreation`.

## [0.0.5] — 2026-08-26

> Note: `0.0.4` was tagged but never reached npm (latest published was `0.0.3`). This release carries the 0.0.4 changes as well.

### Changed

- **License changed from MIT to Apache-2.0.** Apache-2.0 is the license
  Node-RED itself uses. Compared to MIT it adds an explicit patent grant
  (section 3), keeps attribution intact downstream through the new `NOTICE`
  file (section 4d), and requires modified files to be marked as changed
  (section 4b). It remains fully permissive: commercial use, closed-source
  derivatives and forks are all still allowed.
- **`NOTICE` added** and verified to ship inside the npm tarball.
- **Contributing and fork guidance in the README.** Pull requests are welcome,
  including large ones — an issue up front means substantial work can usually
  land here instead of in a parallel package. Forks that are published under
  their own package name are asked to rename their Node-RED node type IDs and
  use their own palette category, so both packages can be installed side by
  side.

## [0.0.4] — 2026-06-22 (Beta)

### Changed

- **Palette grouping.** The three flow nodes (`kafka-suite-producer`,
  `kafka-suite-consumer`, `kafka-suite-admin`) now appear under a dedicated
  **Kafka Suite** category in the Node-RED palette instead of the generic
  `kafka-suite` / `kafka` heading. The two config nodes stay under
  *Configuration nodes*.

### Documentation

- README: added a **Nodes → palette grouping** section with a diagram showing
  the new *Kafka Suite* category and the on-demand config nodes.
- Backfilled the previously missing [0.0.3] changelog entry.

## [0.0.3] — 2026-06-19 (Beta)

### Added

- **SASL/OAUTHBEARER (OAuth 2.0 / OIDC) authentication.** Configurable token
  endpoint with the `password` and `client_credentials` grants (Strimzi /
  Keycloak-style), client id/secret, username/password, scope, audience, and
  per-endpoint TLS verification. Selectable from the broker config node and
  supported on both backends. End-to-end tested against Keycloak + Redpanda.

### Changed

- CI: publish to npm via Trusted Publishing on version tags; the workflow uses
  `npm install` (the repo tracks no committed lockfile, so `npm ci` failed).

### Documentation

- README: refreshed the architecture overview and added an OAUTHBEARER flow
  diagram. Simplified the Mermaid diagrams so GitHub renders them reliably.

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

[0.0.4]: https://github.com/blanpa/node-red-contrib-kafka-suite/releases/tag/v0.0.4
[0.0.3]: https://github.com/blanpa/node-red-contrib-kafka-suite/releases/tag/v0.0.3
[0.0.2]: https://github.com/blanpa/node-red-contrib-kafka-suite/releases/tag/v0.0.2
[0.0.1]: https://github.com/blanpa/node-red-contrib-kafka-suite/releases/tag/v0.0.1
