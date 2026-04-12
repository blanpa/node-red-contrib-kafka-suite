# @blanpa/node-red-contrib-kafka-suite

A comprehensive Apache Kafka integration for Node-RED with full producer, consumer, admin, and schema registry support. Built with a dual-backend abstraction layer (kafkajs + Confluent) and first-class support for all major managed Kafka services.

## Features

- **Producer** with compression (GZIP, Snappy, LZ4, ZSTD), batch mode, headers, and delivery confirmation
- **Consumer** with consumer groups, auto/manual commit, pause/resume, and multiple message formats
- **Admin** operations: create/delete topics, describe cluster, manage consumer groups and offsets
- **Schema Registry** integration with Avro, JSON Schema, and Protobuf (Confluent wire format)
- **Dual backend**: kafkajs (pure JS, zero dependencies) or @confluentinc/kafka-javascript (native, high performance)
- **Service presets**: Confluent Cloud, AWS MSK (IAM + SCRAM), Azure Event Hubs, Aiven, Redpanda
- **Full authentication**: SASL/PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, mutual TLS (mTLS)
- **Robust connection management**: MQTT-style shared connections, auto-reconnect, status propagation
- **Dual output** on producer and admin nodes (success + error) for building error-handling flows

## Installation

```bash
npm install @blanpa/node-red-contrib-kafka-suite
```

Or install via the Node-RED palette manager: search for `@blanpa/node-red-contrib-kafka-suite`.

### Optional dependencies

```bash
# Schema Registry support (Avro, JSON Schema, Protobuf)
npm install @kafkajs/confluent-schema-registry

# High-performance native backend (alternative to kafkajs)
npm install @confluentinc/kafka-javascript
```

## Nodes

### kafka-suite-broker (Config Node)

Shared connection configuration used by all other nodes. Manages the Kafka client lifecycle with automatic connect/disconnect based on registered child nodes.

| Setting | Description |
|---|---|
| Service Preset | Auto-configures auth for managed services (Confluent Cloud, AWS MSK, etc.) |
| Brokers | Comma-separated list of broker addresses (`host:port`) |
| Backend | `kafkajs` (default, pure JS) or `confluent` (native librdkafka) |
| Auth | None, SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, SSL/mTLS |
| SSL | CA cert, client cert, client key, passphrase |

### kafka-suite-producer

Sends messages to Kafka topics with delivery confirmation.

**Inputs** (`msg` properties):

| Property | Type | Description |
|---|---|---|
| `payload` | string/buffer/object/array | Message value. Arrays trigger batch mode. |
| `topic` | string | Target topic (overrides node config) |
| `key` | string | Message key for partitioning |
| `partition` | number | Explicit partition number |
| `headers` | object | Message headers as key-value pairs |

**Outputs**:
- **Output 1 (Success)**: Original message + `msg.kafka` with `{ topic, partition, offset, timestamp, key }`
- **Output 2 (Error)**: Original message + `msg.error` with `{ message, stack }`

### kafka-suite-consumer

Consumes messages from Kafka topics using consumer groups.

**Configuration**:

| Setting | Description |
|---|---|
| Topics | Comma-separated list of topics to subscribe to |
| Group ID | Consumer group ID (auto-generated if empty) |
| Start from | `latest` (new messages only) or `earliest` (all messages) |
| Format | String (UTF-8), JSON (auto-parse), or Raw (Buffer) |
| Auto Commit | Enable/disable automatic offset commits |
| Concurrency | Number of partitions to consume in parallel |

**Output** (`msg` properties):

| Property | Type | Description |
|---|---|---|
| `payload` | any | Decoded message value |
| `topic` | string | Source topic |
| `key` | string | Message key |
| `partition` | number | Source partition |
| `offset` | string | Message offset |
| `timestamp` | string | Message timestamp |
| `headers` | object | Message headers |
| `commit()` | function | Manual commit callback (when auto-commit is off) |

**Control input**: Send `msg.action = "pause"` or `msg.action = "resume"` to control consumption.

### kafka-suite-admin

Performs Kafka cluster administration operations. Set `msg.action` to one of:

| Action | Required msg properties | Description |
|---|---|---|
| `listTopics` | - | List all topics |
| `createTopic` | `topic`, `config.partitions`, `config.replicationFactor` | Create a topic |
| `deleteTopic` | `topic` | Delete topic(s) |
| `describeCluster` | - | Get cluster info |
| `listGroups` | - | List consumer groups |
| `describeGroup` | `groupId` | Get group details |
| `fetchTopicOffsets` | `topic` | Get partition offsets |
| `resetOffsets` | `groupId`, `topic` | Reset consumer group offsets |
| `deleteGroup` | `groupId` | Delete consumer group(s) |

### kafka-suite-schema-registry (Config Node)

Confluent Schema Registry connection. When referenced by a producer or consumer node, messages are automatically encoded/decoded using the Confluent wire format.

Supports Avro, JSON Schema, and Protobuf. Requires `@kafkajs/confluent-schema-registry`.

## Backend Selection

| Backend | Pros | Cons |
|---|---|---|
| **kafkajs** (default) | Pure JS, zero native deps, works on ARM/RPi/Docker, simple install | Unmaintained since 2023, no Kafka 4.0 support |
| **confluent** | Actively maintained by Confluent, Kafka 4.0 ready, high performance | Requires native C compilation, larger install |

Select the backend in the broker config node. The confluent backend requires `@confluentinc/kafka-javascript` to be installed separately.

## Managed Service Configuration

### Confluent Cloud
- Service preset: **Confluent Cloud**
- Auth: SASL/PLAIN (API Key as username, API Secret as password)
- SSL is enabled automatically

### AWS MSK (IAM)
- Service preset: **AWS MSK (IAM)**
- Auth: SASL/OAUTHBEARER with IAM token provider

### AWS MSK (SCRAM)
- Service preset: **AWS MSK (SCRAM)**
- Auth: SASL/SCRAM-SHA-512

### Azure Event Hubs
- Service preset: **Azure Event Hubs**
- Auth: SASL/PLAIN (username: `$ConnectionString`, password: connection string)
- Port: 9093

### Aiven
- Service preset: **Aiven**
- Auth: Mutual TLS (CA cert + client cert + client key)

### Redpanda
- Service preset: **Redpanda**
- Fully Kafka-compatible, same config as self-hosted

## Development

### Setup

```bash
git clone https://github.com/blanpa/node-red-contrib-kafka-suite.git
cd node-red-contrib-kafka-suite
npm install
```

### Integration Tests

Start a local Kafka broker via Docker and run the test suite:

```bash
docker compose up -d kafka
npm run test:integration
```

### Docker Compose (full stack)

Start Kafka, Schema Registry, and Node-RED with the nodes pre-installed:

```bash
docker compose up
```

Then open Node-RED at `http://localhost:1880` and import the example flow from `examples/test-all-nodes.json`.

## License

MIT
