'use strict';

module.exports = function (RED) {
  function KafkaAdminNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.brokerNode = RED.nodes.getNode(config.broker);
    node.admin = null;

    if (!node.brokerNode) {
      node.status({ fill: 'red', shape: 'ring', text: 'no broker configured' });
      return;
    }

    node._setupAdmin = async function () {
      try {
        const client = node.brokerNode.getClient();
        if (!client) return false;
        node.admin = client.createAdmin();
        await node.admin.connect();
        node.status({ fill: 'green', shape: 'dot', text: 'ready' });
        return true;
      } catch (err) {
        if (node.admin) {
          try { await node.admin.disconnect(); } catch (e) { /* ignore */ }
          node.admin = null;
        }
        node.status({ fill: 'red', shape: 'ring', text: 'admin error' });
        node.error('Failed to connect admin: ' + err.message);
        return false;
      }
    };

    node.brokerNode.register(node);

    const checkInterval = setInterval(async () => {
      if (node.brokerNode.connected && !node.admin) {
        await node._setupAdmin();
      }
    }, 500);

    // Handle admin operations
    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      done = done || function (err) { if (err) node.error(err, msg); };

      if (!node.admin) {
        done(new Error('Admin not connected'));
        return;
      }

      const action = msg.action;
      if (!action) {
        done(new Error('No action specified. Set msg.action (e.g., "listTopics", "createTopic")'));
        return;
      }

      try {
        let result;

        switch (action) {
          case 'listTopics':
            result = await node.admin.listTopics();
            break;

          case 'createTopic':
            if (!msg.topic) throw new Error('msg.topic is required for createTopic');
            result = await node.admin.createTopics([{
              topic: msg.topic,
              numPartitions: (msg.config && msg.config.partitions) || msg.partitions || 1,
              replicationFactor: (msg.config && msg.config.replicationFactor) || msg.replicationFactor || 1,
              configEntries: (msg.config && msg.config.configEntries) || []
            }]);
            break;

          case 'deleteTopic':
            if (!msg.topic) throw new Error('msg.topic is required for deleteTopic');
            const topics = Array.isArray(msg.topic) ? msg.topic : [msg.topic];
            result = await node.admin.deleteTopics(topics);
            break;

          case 'describeCluster':
            result = await node.admin.describeCluster();
            break;

          case 'listGroups':
            result = await node.admin.listGroups();
            break;

          case 'describeGroup':
            if (!msg.groupId) throw new Error('msg.groupId is required for describeGroup');
            const groupIds = Array.isArray(msg.groupId) ? msg.groupId : [msg.groupId];
            result = await node.admin.describeGroups(groupIds);
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
            const delGroups = Array.isArray(msg.groupId) ? msg.groupId : [msg.groupId];
            result = await node.admin.deleteGroups(delGroups);
            break;

          default:
            throw new Error('Unknown action: ' + action + '. Supported: listTopics, createTopic, deleteTopic, describeCluster, listGroups, describeGroup, fetchTopicOffsets, resetOffsets, deleteGroup');
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
      clearInterval(checkInterval);
      const cleanup = async () => {
        if (node.admin) {
          try { await node.admin.disconnect(); } catch (e) { /* ignore */ }
          node.admin = null;
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

  RED.nodes.registerType('kafka-suite-admin', KafkaAdminNode);
};
