'use strict';

module.exports = function (RED) {
  function KafkaProducerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Config
    node.topic = config.topic || '';
    node.key = config.key || '';
    node.partition = config.partition || '';
    node.compression = config.compression || 'none';
    node.acks = config.acks != null ? parseInt(config.acks) : -1;
    node.timeout = parseInt(config.timeout) || 30000;
    node.schemaRegistry = config.schemaRegistry || '';

    // Broker config node
    node.brokerNode = RED.nodes.getNode(config.broker);
    node.producer = null;

    // Schema registry config node (optional)
    node.registryNode = node.schemaRegistry ? RED.nodes.getNode(node.schemaRegistry) : null;

    if (!node.brokerNode) {
      node.status({ fill: 'red', shape: 'ring', text: 'no broker configured' });
      return;
    }

    // Wait for broker connection then create producer
    node._setupProducer = async function () {
      try {
        const client = node.brokerNode.getClient();
        if (!client) {
          return false;
        }
        node.producer = client.createProducer({
          allowAutoTopicCreation: true
        });
        await node.producer.connect();
        node.status({ fill: 'green', shape: 'dot', text: 'ready' });
        node.log('Kafka producer connected');
        return true;
      } catch (err) {
        node.producer = null;
        node.status({ fill: 'red', shape: 'ring', text: 'producer error' });
        node.error('Failed to connect producer: ' + err.message);
        return false;
      }
    };

    // Register with broker
    node.brokerNode.register(node);

    // Poll until broker is connected and producer setup succeeds.
    // Stays alive so transient setup failures (e.g. Kafka still booting)
    // are retried instead of leaving the node permanently dead.
    const checkInterval = setInterval(async () => {
      if (node.brokerNode.connected && !node.producer) {
        await node._setupProducer();
      }
    }, 500);

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

        const key = node.key || msg.key || null;
        const hasConfigPartition = node.partition !== '' && node.partition != null;
        const partition = hasConfigPartition
          ? parseInt(node.partition)
          : (msg.partition != null ? msg.partition : undefined);
        const headers = msg.headers || undefined;

        const useRegistry = !!(node.registryNode && node.registryNode.encode);
        const subject = msg.schema || topic + '-value';

        // Optional auto-registration: if the broker config has autoRegister=true
        // and the message provides a schemaDefinition, register it before encoding.
        // This lets flows ship a schema with the first message rather than having
        // to pre-register out of band.
        if (useRegistry && msg.schemaDefinition && node.registryNode.autoRegister && node.registryNode.registerSchema) {
          try {
            await node.registryNode.registerSchema(subject, msg.schemaDefinition, msg.schemaType);
          } catch (regErr) {
            done(new Error('Schema auto-register failed for ' + subject + ': ' + regErr.message));
            return;
          }
        }

        const encodeOne = async (v) => useRegistry ? await node.registryNode.encode(subject, v) : v;

        // Batch mode: if payload is an array, send as batch.
        // Items can be plain values or objects { key, value, headers, partition }
        // — per-item fields override the node-level defaults.
        // When a Schema Registry is configured, encode EACH element separately
        // — encoding the whole array would validate it against the value
        // schema as if it were a single record (which fails for record/struct
        // schemas).
        let result;
        if (Array.isArray(msg.payload)) {
          const messages = await Promise.all(msg.payload.map(async (item) => {
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
                headers: item.headers || headers,
                partition: item.partition != null ? item.partition : partition
              };
            }
            return { key, value: await encodeOne(item), headers, partition };
          }));
          result = await node.producer.send({
            topic,
            messages,
            acks: node.acks,
            timeout: node.timeout,
            compression: node.compression
          });
        } else {
          const value = await encodeOne(msg.payload);
          result = await node.producer.send({
            topic,
            messages: [{ key, value, headers, partition }],
            acks: node.acks,
            timeout: node.timeout,
            compression: node.compression
          });
        }

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
      clearInterval(checkInterval);
      const cleanup = async () => {
        if (node.producer) {
          try {
            await node.producer.disconnect();
          } catch (err) {
            node.warn('Error disconnecting producer: ' + err.message);
          }
          node.producer = null;
        }
        if (node.brokerNode) {
          node.brokerNode.deregister(node, done, removed);
        } else {
          done();
        }
      };
      cleanup();
    });
  }

  RED.nodes.registerType('kafka-suite-producer', KafkaProducerNode);
};
