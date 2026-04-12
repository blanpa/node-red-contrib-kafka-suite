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
          // Broker not yet connected, will retry via status change
          return;
        }
        node.producer = client.createProducer({
          allowAutoTopicCreation: true
        });
        await node.producer.connect();
        node.status({ fill: 'green', shape: 'dot', text: 'ready' });
        node.log('Kafka producer connected');
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'producer error' });
        node.error('Failed to connect producer: ' + err.message);
      }
    };

    // Register with broker
    node.brokerNode.register(node);

    // Setup producer once broker is connected
    const checkInterval = setInterval(() => {
      if (node.brokerNode.connected && !node.producer) {
        clearInterval(checkInterval);
        node._setupProducer();
      }
    }, 500);

    // Handle incoming messages
    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      done = done || function (err) { if (err) node.error(err, msg); };

      if (!node.producer) {
        const errMsg = Object.assign({}, msg, { error: { message: 'Producer not connected' } });
        send([null, errMsg]);
        done(new Error('Producer not connected'));
        return;
      }

      try {
        // Resolve topic, key, partition from config or msg
        const topic = node.topic || msg.topic;
        if (!topic) {
          const errMsg = Object.assign({}, msg, { error: { message: 'No topic specified' } });
          send([null, errMsg]);
          done(new Error('No topic specified. Set topic in node config or msg.topic'));
          return;
        }

        const key = node.key || msg.key || null;
        const partition = node.partition !== '' ? parseInt(node.partition) : (msg.partition != null ? msg.partition : undefined);
        const headers = msg.headers || undefined;

        // Prepare value
        let value = msg.payload;

        // Schema Registry encoding (if configured)
        if (node.registryNode && node.registryNode.encode) {
          const subject = msg.schema || topic + '-value';
          value = await node.registryNode.encode(subject, value);
        }

        // Batch mode: if payload is an array, send as batch
        let result;
        if (Array.isArray(value)) {
          const messages = value.map(v => ({
            key: key,
            value: v,
            headers: headers,
            partition: partition
          }));
          result = await node.producer.send({
            topic,
            messages,
            acks: node.acks,
            timeout: node.timeout,
            compression: node.compression
          });
        } else {
          result = await node.producer.send({
            topic,
            messages: [{ key, value, headers, partition }],
            acks: node.acks,
            timeout: node.timeout,
            compression: node.compression
          });
        }

        // Success output - kafkajs returns array of RecordMetadata per partition
        const firstResult = Array.isArray(result) && result.length > 0 ? result[0] : {};
        msg.kafka = {
          topic: topic,
          partition: firstResult.partition,
          offset: firstResult.baseOffset,
          timestamp: Date.now(),
          key: key
        };
        send([msg, null]);
        done();
      } catch (err) {
        const errMsg = Object.assign({}, msg, {
          error: { message: err.message, stack: err.stack }
        });
        send([null, errMsg]);
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
