'use strict';

module.exports = function (RED) {
  function KafkaConsumerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Config
    node.topics = (config.topics || '').split(',').map(t => t.trim()).filter(Boolean);
    node.groupId = config.groupId || 'node-red-' + node.id;
    node.startOffset = config.startOffset || 'latest';
    node.autoCommit = config.autoCommit !== false;
    node.autoCommitInterval = parseInt(config.autoCommitInterval) || 5000;
    node.messageFormat = config.messageFormat || 'string';
    node.concurrency = parseInt(config.concurrency) || 1;
    node.schemaRegistry = config.schemaRegistry || '';

    // Broker config node
    node.brokerNode = RED.nodes.getNode(config.broker);
    node.consumer = null;
    node.paused = false;

    // Schema registry config node (optional)
    node.registryNode = node.schemaRegistry ? RED.nodes.getNode(node.schemaRegistry) : null;

    if (!node.brokerNode) {
      node.status({ fill: 'red', shape: 'ring', text: 'no broker configured' });
      return;
    }

    if (node.topics.length === 0) {
      node.status({ fill: 'red', shape: 'ring', text: 'no topic configured' });
      return;
    }

    // Setup consumer
    node._setupConsumer = async function () {
      try {
        const client = node.brokerNode.getClient();
        if (!client) return false;

        node.consumer = client.createConsumer(node.groupId, {
          sessionTimeout: 30000,
          heartbeatInterval: 3000
        });

        // Consumer events
        node.consumer.on('rebalancing', () => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'rebalancing...' });
        });
        node.consumer.on('ready', () => {
          node.status({ fill: 'green', shape: 'dot', text: 'consuming' });
        });

        await node.consumer.connect();
        await node.consumer.subscribe({
          topics: node.topics,
          fromBeginning: node.startOffset === 'earliest'
        });

        await node.consumer.run({
          eachMessage: async ({ topic, partition, message, heartbeat }) => {
            try {
              let value = message.value;

              // Decode based on message format
              if (node.registryNode && node.registryNode.decode) {
                value = await node.registryNode.decode(value);
              } else {
                value = node._decodeValue(value);
              }

              // Build headers object. KafkaJS may deliver header values as
              // Buffer, string, null, or Array (for multi-value headers).
              const decodeHeader = (v) => {
                if (v == null) return null;
                if (Array.isArray(v)) return v.map(decodeHeader);
                if (Buffer.isBuffer(v)) return v.toString();
                return String(v);
              };
              let headers = {};
              if (message.headers) {
                for (const [hKey, hVal] of Object.entries(message.headers)) {
                  headers[hKey] = decodeHeader(hVal);
                }
              }

              // Build output message
              const msg = {
                payload: value,
                topic: topic,
                key: message.key ? message.key.toString() : null,
                partition: partition,
                offset: message.offset,
                timestamp: message.timestamp,
                headers: headers,
                kafka: {
                  consumerGroup: node.groupId
                }
              };

              // Manual commit callback. Use BigInt because Kafka offsets are
              // 64-bit unsigned integers and can exceed Number.MAX_SAFE_INTEGER
              // (2^53-1) on busy long-running topics.
              if (!node.autoCommit) {
                msg.commit = async function () {
                  await node.consumer.commitOffsets([{
                    topic: topic,
                    partition: partition,
                    offset: (BigInt(message.offset) + 1n).toString()
                  }]);
                };
              }

              node.send(msg);
            } catch (err) {
              node.error('Error processing message: ' + err.message, {
                topic, partition, offset: message.offset
              });
            }
          },
          autoCommit: node.autoCommit,
          autoCommitInterval: node.autoCommitInterval,
          partitionsConsumedConcurrently: node.concurrency
        });

        node.status({ fill: 'green', shape: 'dot', text: 'consuming' });
        node.log('Kafka consumer started for topics: ' + node.topics.join(', '));
        return true;
      } catch (err) {
        // Reset so the next interval tick can retry instead of leaving
        // the node stuck with a half-initialized consumer.
        if (node.consumer) {
          try { await node.consumer.disconnect(); } catch (e) { /* ignore */ }
          node.consumer = null;
        }
        node.status({ fill: 'red', shape: 'ring', text: 'consumer error' });
        node.error('Failed to start consumer: ' + err.message);
        return false;
      }
    };

    // Register with broker
    node.brokerNode.register(node);

    // Poll until broker is connected and consumer setup succeeds.
    const checkInterval = setInterval(async () => {
      if (node.brokerNode.connected && !node.consumer) {
        await node._setupConsumer();
      }
    }, 500);

    // Handle control messages (pause/resume)
    node.on('input', function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      done = done || function (err) { if (err) node.error(err, msg); };

      if (!node.consumer) {
        done(new Error('Consumer not connected'));
        return;
      }

      const action = msg.action || msg.payload;
      try {
        if (action === 'pause' && !node.paused) {
          node.consumer.pause(node.topics);
          node.paused = true;
          node.status({ fill: 'blue', shape: 'dot', text: 'paused' });
        } else if (action === 'resume' && node.paused) {
          node.consumer.resume(node.topics);
          node.paused = false;
          node.status({ fill: 'green', shape: 'dot', text: 'consuming' });
        }
        done();
      } catch (err) {
        done(err);
      }
    });

    // Decode value based on configured format
    node._decodeValue = function (value) {
      if (!value) return null;

      switch (node.messageFormat) {
        case 'raw':
          return value;
        case 'json':
          try {
            return JSON.parse(value.toString());
          } catch (e) {
            return value.toString();
          }
        case 'string':
        default:
          return value.toString();
      }
    };

    // Close handler
    node.on('close', function (removed, done) {
      clearInterval(checkInterval);
      const cleanup = async () => {
        if (node.consumer) {
          try {
            await node.consumer.disconnect();
          } catch (err) {
            node.warn('Error disconnecting consumer: ' + err.message);
          }
          node.consumer = null;
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

  RED.nodes.registerType('kafka-suite-consumer', KafkaConsumerNode);
};
