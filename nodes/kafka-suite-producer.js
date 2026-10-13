'use strict';

const { ChildLifecycle } = require('../lib/child-lifecycle');
const { intOr, toKafkaHeaders } = require('../lib/util');

module.exports = function (RED) {
  function KafkaProducerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Config
    node.topic = config.topic || '';
    node.key = config.key || '';
    node.partition = config.partition || '';
    node.compression = config.compression || 'none';
    node.acks = intOr(config.acks, -1);
    node.timeout = intOr(config.timeout, 30000, 1);
    node.schemaRegistry = config.schemaRegistry || '';
    // Defaults to true: configs saved before this option existed relied on it.
    node.autoCreateTopics = config.autoCreateTopics !== false;
    // msg property carrying the Kafka headers. Configurable because
    // msg.headers is also used by the HTTP nodes.
    node.headersProperty = config.headersProperty || 'headers';

    // Broker config node
    node.brokerNode = RED.nodes.getNode(config.broker);
    node.producer = null;

    // Schema registry config node (optional)
    node.registryNode = node.schemaRegistry ? RED.nodes.getNode(node.schemaRegistry) : null;

    if (!node.brokerNode) {
      node.status({ fill: 'red', shape: 'ring', text: 'no broker configured' });
      return;
    }

    node.lifecycle = new ChildLifecycle(node, {
      label: 'producer',
      create: (client) => client.createProducer({
        allowAutoTopicCreation: node.autoCreateTopics,
        acks: node.acks,
        timeout: node.timeout,
        compression: node.compression
      }),
      start: (producer) => producer.connect(),
      attach: (producer) => {
        node.producer = producer;
        node.status({ fill: 'green', shape: 'dot', text: 'ready' });
        node.log('Kafka producer connected');
      },
      detach: () => { node.producer = null; }
    });

    // Called by the broker node once it is (re)connected.
    node._onBrokerConnected = () => node.lifecycle.start();

    // Register with broker
    node.brokerNode.register(node);

    // Handle incoming messages
    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      done = done || function (err) { if (err) node.error(err, msg); };

      if (!node.producer) {
        done(new Error('Producer not connected'));
        return;
      }

      try {
        // Resolve topic, key, partition from config or msg
        const topic = node.topic || msg.topic;
        if (!topic) {
          done(new Error('No topic specified. Set topic in node config or msg.topic'));
          return;
        }

        const key = node.key !== '' ? node.key : (msg.key != null ? msg.key : null);
        const hasConfigPartition = node.partition !== '' && node.partition != null;
        const partition = hasConfigPartition
          ? parseInt(node.partition, 10)
          : (msg.partition != null ? msg.partition : undefined);
        const headers = toKafkaHeaders(RED.util.getMessageProperty(msg, node.headersProperty));

        const useRegistry = !!(node.registryNode && node.registryNode.encode);
        const subject = msg.schema || topic + '-value';

        // Optional auto-registration: if the registry config has autoRegister=true
        // and the message provides a schemaDefinition, register it and encode
        // with exactly that schema. The registry node caches registrations, so
        // repeating the same definition on every message costs no round-trip.
        let schemaId;
        if (useRegistry && msg.schemaDefinition && node.registryNode.autoRegister && node.registryNode.registerSchema) {
          try {
            schemaId = await node.registryNode.registerSchema(subject, msg.schemaDefinition, msg.schemaType);
          } catch (regErr) {
            done(new Error('Schema auto-register failed for ' + subject + ': ' + regErr.message));
            return;
          }
        }

        const encodeOne = async (v) => useRegistry ? await node.registryNode.encode(subject, v, schemaId) : v;

        // Batch mode: if payload is an array, send as batch.
        // Items can be plain values or objects { key, value, headers, partition }
        // — per-item fields override the node-level defaults.
        // When a Schema Registry is configured, encode EACH element separately
        // — encoding the whole array would validate it against the value
        // schema as if it were a single record (which fails for record/struct
        // schemas).
        let messages;
        if (Array.isArray(msg.payload)) {
          messages = await Promise.all(msg.payload.map(async (item) => {
            // An item is treated as a Kafka envelope only when it carries
            // metadata (key/headers/partition) AND a value field. A plain
            // record like { value: 1.2, sensor: "x" } stays a record so its
            // own "value" field isn't accidentally extracted.
            const isWrapped = item && typeof item === 'object' && !Buffer.isBuffer(item)
              && Object.prototype.hasOwnProperty.call(item, 'value')
              && (Object.prototype.hasOwnProperty.call(item, 'key')
                || Object.prototype.hasOwnProperty.call(item, 'headers')
                || Object.prototype.hasOwnProperty.call(item, 'partition'));
            if (isWrapped) {
              return {
                key: item.key != null ? item.key : key,
                value: await encodeOne(item.value),
                headers: item.headers ? toKafkaHeaders(item.headers) : headers,
                partition: item.partition != null ? item.partition : partition
              };
            }
            return { key, value: await encodeOne(item), headers, partition };
          }));
        } else {
          messages = [{ key, value: await encodeOne(msg.payload), headers, partition }];
        }

        const result = await node.producer.send({
          topic,
          messages,
          acks: node.acks,
          timeout: node.timeout,
          compression: node.compression
        });

        // Success output - kafkajs returns array of RecordMetadata per partition.
        // We expose the first entry on `msg.kafka` for backwards compat AND the
        // full array on `msg.kafka.results` so batch sends across multiple
        // partitions don't lose information.
        const resultsArr = Array.isArray(result) ? result : [];
        const firstResult = resultsArr[0] || {};
        msg.kafka = {
          topic: topic,
          partition: firstResult.partition,
          offset: firstResult.baseOffset,
          timestamp: Date.now(),
          key: key,
          results: resultsArr
        };
        send(msg);
        done();
      } catch (err) {
        done(err);
      }
    });

    // Close handler
    node.on('close', function (removed, done) {
      node.lifecycle.stop().then(() => {
        node.brokerNode.deregister(node, done, removed);
      });
    });
  }

  RED.nodes.registerType('kafka-suite-producer', KafkaProducerNode);
};
