'use strict';

const { ChildLifecycle } = require('../lib/child-lifecycle');

const ACTIONS = [
  'listTopics', 'createTopic', 'deleteTopic', 'describeCluster', 'listGroups',
  'describeGroup', 'fetchTopicOffsets', 'resetOffsets', 'deleteGroup'
];
// Actions that delete data or move consumer positions.
const DESTRUCTIVE_ACTIONS = ['deleteTopic', 'deleteGroup', 'resetOffsets'];

module.exports = function (RED) {
  function KafkaAdminNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Fixed action from the config; empty means "take it from msg.action".
    node.action = config.action || '';
    // New nodes default to false (see the editor defaults); configs saved
    // before this option existed keep their previous behavior.
    node.allowDestructive = config.allowDestructive !== false;

    node.brokerNode = RED.nodes.getNode(config.broker);
    node.admin = null;

    if (!node.brokerNode) {
      node.status({ fill: 'red', shape: 'ring', text: 'no broker configured' });
      return;
    }

    node.lifecycle = new ChildLifecycle(node, {
      label: 'admin',
      create: (client) => client.createAdmin(),
      start: (admin) => admin.connect(),
      attach: (admin) => {
        node.admin = admin;
        node.status({ fill: 'green', shape: 'dot', text: 'ready' });
      },
      detach: () => { node.admin = null; }
    });

    // Called by the broker node once it is (re)connected.
    node._onBrokerConnected = () => node.lifecycle.start();

    node.brokerNode.register(node);

    // Handle admin operations
    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      done = done || function (err) { if (err) node.error(err, msg); };

      if (!node.admin) {
        done(new Error('Admin not connected'));
        return;
      }

      const action = node.action || msg.action;
      if (!action) {
        done(new Error('No action specified. Set msg.action (e.g., "listTopics", "createTopic")'));
        return;
      }
      if (!ACTIONS.includes(action)) {
        done(new Error('Unknown action: ' + action + '. Supported: ' + ACTIONS.join(', ')));
        return;
      }
      if (DESTRUCTIVE_ACTIONS.includes(action) && !node.allowDestructive) {
        done(new Error('Action "' + action + '" is destructive and disabled on this node. ' +
          'Enable "Allow destructive actions" in the node config to use it.'));
        return;
      }

      try {
        let result;

        switch (action) {
          case 'listTopics':
            result = await node.admin.listTopics();
            break;

          case 'createTopic': {
            if (!msg.topic) throw new Error('msg.topic is required for createTopic');
            const cfg = msg.config || {};
            // -1 = use the broker defaults (num.partitions /
            // default.replication.factor), which works on managed clusters
            // that require a replication factor of 3.
            result = await node.admin.createTopics([{
              topic: msg.topic,
              numPartitions: firstNumber(cfg.partitions, msg.partitions, -1),
              replicationFactor: firstNumber(cfg.replicationFactor, msg.replicationFactor, -1),
              configEntries: cfg.configEntries || []
            }]);
            break;
          }

          case 'deleteTopic':
            if (!msg.topic) throw new Error('msg.topic is required for deleteTopic');
            result = await node.admin.deleteTopics(toArray(msg.topic));
            break;

          case 'describeCluster':
            result = await node.admin.describeCluster();
            break;

          case 'listGroups':
            result = await node.admin.listGroups();
            break;

          case 'describeGroup':
            if (!msg.groupId) throw new Error('msg.groupId is required for describeGroup');
            result = await node.admin.describeGroups(toArray(msg.groupId));
            break;

          case 'fetchTopicOffsets':
            if (!msg.topic) throw new Error('msg.topic is required for fetchTopicOffsets');
            result = await node.admin.fetchTopicOffsets(msg.topic);
            break;

          case 'resetOffsets':
            if (!msg.groupId || !msg.topic) throw new Error('msg.groupId and msg.topic are required for resetOffsets');
            result = await node.admin.resetOffsets({
              groupId: msg.groupId,
              topic: msg.topic,
              earliest: msg.earliest !== false
            });
            break;

          case 'deleteGroup':
            if (!msg.groupId) throw new Error('msg.groupId is required for deleteGroup');
            result = await node.admin.deleteGroups(toArray(msg.groupId));
            break;
        }

        msg.payload = result;
        msg.action = action;
        send(msg);
        done();
      } catch (err) {
        done(err);
      }
    });

    node.on('close', function (removed, done) {
      node.lifecycle.stop().then(() => {
        node.brokerNode.deregister(node, done, removed);
      });
    });
  }

  function toArray(v) {
    return Array.isArray(v) ? v : [v];
  }

  // First argument that is a finite number (numeric strings accepted).
  function firstNumber(...values) {
    for (const v of values) {
      if (v === '' || v == null) continue;
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
    return undefined;
  }

  RED.nodes.registerType('kafka-suite-admin', KafkaAdminNode);
};
