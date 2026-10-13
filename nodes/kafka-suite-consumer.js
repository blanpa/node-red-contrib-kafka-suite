'use strict';

const { ChildLifecycle } = require('../lib/child-lifecycle');
const { intOr } = require('../lib/util');

module.exports = function (RED) {
  function KafkaConsumerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Config
    node.topics = (config.topics || '').split(',').map(t => t.trim()).filter(Boolean);
    node.groupId = config.groupId || 'node-red-' + node.id;
    node.startOffset = config.startOffset || 'latest';
    node.autoCommit = config.autoCommit !== false;
    node.autoCommitInterval = intOr(config.autoCommitInterval, 5000, 1);
    node.messageFormat = config.messageFormat || 'string';
    node.keyFormat = config.keyFormat || 'string';
    node.concurrency = intOr(config.concurrency, 1, 1);
    node.sessionTimeout = intOr(config.sessionTimeout, 30000, 1);
    node.heartbeatInterval = intOr(config.heartbeatInterval, 3000, 1);
    // msg property receiving the Kafka headers. Configurable because
    // msg.headers is also used by the HTTP nodes.
    node.headersProperty = config.headersProperty || 'headers';
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

    const runningStatus = () => node.paused
      ? { fill: 'blue', shape: 'dot', text: 'paused' }
      : { fill: 'green', shape: 'dot', text: 'consuming' };

    node.lifecycle = new ChildLifecycle(node, {
      label: 'consumer',
      create: (client) => client.createConsumer(node.groupId, {
        sessionTimeout: node.sessionTimeout,
        heartbeatInterval: node.heartbeatInterval
      }),
      start: async (consumer) => {
        consumer.on('rebalancing', () => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'rebalancing...' });
        });
        consumer.on('ready', () => node.status(runningStatus()));
        consumer.on('crash', (e) => node._onCrash(consumer, e || {}));

        await consumer.connect();
        await consumer.subscribe({
          topics: node.topics,
          fromBeginning: node.startOffset === 'earliest'
        });
        await consumer.run({
          eachMessage: (args) => node._handleMessage(consumer, args),
          autoCommit: node.autoCommit,
          autoCommitInterval: node.autoCommitInterval,
          partitionsConsumedConcurrently: node.concurrency
        });
        // Keep a pause across a restart of the consumer.
        if (node.paused) consumer.pause(node.topics);
      },
      attach: (consumer) => {
        node.consumer = consumer;
        node.status(runningStatus());
        node.log('Kafka consumer started for topics: ' + node.topics.join(', '));
      },
      detach: () => { node.consumer = null; }
    });

    // Called by the broker node once it is (re)connected.
    node._onBrokerConnected = () => node.lifecycle.start();

    /**
     * The client library reports a consumer crash. With restart=true it
     * recovers on its own; otherwise the consumer is dead and we rebuild it
     * (with backoff) instead of showing "consuming" for a stopped consumer.
     */
    node._onCrash = function (consumer, { error, restart }) {
      if (node.lifecycle.resource !== consumer) return;
      const reason = error && error.message ? error.message : 'unknown error';
      if (restart) {
        node.status({ fill: 'yellow', shape: 'ring', text: 'restarting...' });
        node.warn('Kafka consumer crashed, restarting: ' + reason);
        return;
      }
      node.status({ fill: 'red', shape: 'ring', text: 'consumer crashed' });
      node.error('Kafka consumer crashed: ' + reason + ' (restarting)');
      node.lifecycle.restart();
    };

    // Build headers object. KafkaJS may deliver header values as
    // Buffer, string, null, or Array (for multi-value headers).
    const decodeHeader = (v) => {
      if (v == null) return null;
      if (Array.isArray(v)) return v.map(decodeHeader);
      if (Buffer.isBuffer(v)) return v.toString();
      return String(v);
    };

    node._handleMessage = async function (consumer, { topic, partition, message }) {
      try {
        let value = message.value;

        // Decode based on message format
        if (node.registryNode && node.registryNode.decode) {
          value = await node.registryNode.decode(value);
        } else {
          value = node._decodeValue(value);
        }

        const headers = {};
        if (message.headers) {
          for (const [hKey, hVal] of Object.entries(message.headers)) {
            headers[hKey] = decodeHeader(hVal);
          }
        }

        let key = null;
        if (message.key != null) {
          key = node.keyFormat === 'raw' ? message.key : message.key.toString();
        }

        // Build output message
        const msg = {
          payload: value,
          topic: topic,
          key: key,
          partition: partition,
          offset: message.offset,
          timestamp: message.timestamp,
          kafka: {
            consumerGroup: node.groupId
          }
        };
        RED.util.setMessageProperty(msg, node.headersProperty, headers, true);

        // Manual commit callback. Use BigInt because Kafka offsets are
        // 64-bit unsigned integers and can exceed Number.MAX_SAFE_INTEGER
        // (2^53-1) on busy long-running topics.
        if (!node.autoCommit) {
          msg.commit = async function () {
            await consumer.commitOffsets([nextOffset(topic, partition, message.offset)]);
          };
        }

        node.send(msg);
      } catch (err) {
        node.error('Error processing message: ' + err.message, {
          topic, partition, offset: message.offset
        });
      }
    };

    // Register with broker
    node.brokerNode.register(node);

    // Handle control messages: pause, resume, commit
    node.on('input', async function (msg, send, done) {
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
          node.status(runningStatus());
        } else if (action === 'resume' && node.paused) {
          node.consumer.resume(node.topics);
          node.paused = false;
          node.status(runningStatus());
        } else if (action === 'commit') {
          // Commit a processed message by feeding it back with
          // msg.action = 'commit' (it still carries topic/partition/offset).
          if (msg.topic == null || msg.partition == null || msg.offset == null) {
            throw new Error('commit requires msg.topic, msg.partition and msg.offset');
          }
          await node.consumer.commitOffsets([nextOffset(msg.topic, msg.partition, msg.offset)]);
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
      node.lifecycle.stop().then(() => {
        node.brokerNode.deregister(node, done, removed);
      });
    });
  }

  // Kafka commits the offset of the NEXT message to read.
  function nextOffset(topic, partition, offset) {
    return {
      topic: topic,
      partition: Number(partition),
      offset: (BigInt(offset) + 1n).toString()
    };
  }

  RED.nodes.registerType('kafka-suite-consumer', KafkaConsumerNode);
};
