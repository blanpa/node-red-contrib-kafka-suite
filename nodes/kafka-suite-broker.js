'use strict';

const { createClient } = require('../lib/client-factory');

module.exports = function (RED) {
  function KafkaBrokerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Config properties
    node.name = config.name;
    node.brokers = (config.brokers || 'localhost:9092').split(',').map(b => b.trim()).filter(Boolean);
    node.clientId = config.clientId || 'node-red-kafka-suite';

    // Validate broker URLs early. Accepted forms:
    //   host:port              (most common)
    //   PROTOCOL://host:port   (e.g. SASL_SSL://kafka.example.com:9094)
    // Anything else (missing port, whitespace, invalid host) is rejected.
    const brokerRegex = /^(?:[A-Z_]+:\/\/)?[a-zA-Z0-9.-]+:\d{1,5}$/;
    node.invalidBrokers = node.brokers.filter(b => !brokerRegex.test(b));
    node.backend = config.backend || 'kafkajs';
    node.servicePreset = config.servicePreset || 'self-hosted';
    node.authType = config.authType || 'none';
    node.logLevel = config.logLevel || 'warn';
    node.connectionTimeout = parseInt(config.connectionTimeout) || 3000;
    node.requestTimeout = parseInt(config.requestTimeout) || 30000;
    node.maxRetryTime = parseInt(config.maxRetryTime) || 30000;
    node.initialRetryTime = parseInt(config.initialRetryTime) || 300;
    node.retries = parseInt(config.retries) || 5;
    node.sslRejectUnauthorized = config.sslRejectUnauthorized !== false;

    // User registry (MQTT-style pattern)
    node.users = {};
    node.client = null;
    node.connected = false;
    node.connecting = false;
    node.closing = false;

    /**
     * Build adapter config from node config + credentials
     */
    node.getAdapterConfig = function () {
      const adapterConfig = {
        clientId: node.clientId,
        brokers: node.brokers,
        connectionTimeout: node.connectionTimeout,
        requestTimeout: node.requestTimeout,
        maxRetryTime: node.maxRetryTime,
        initialRetryTime: node.initialRetryTime,
        retries: node.retries,
        logLevel: node.logLevel
      };

      // SSL configuration
      const needsSSL = ['sasl-plain', 'sasl-scram-256', 'sasl-scram-512', 'sasl-oauthbearer', 'ssl'].includes(node.authType);
      if (needsSSL) {
        adapterConfig.ssl = true;
        adapterConfig.sslRejectUnauthorized = node.sslRejectUnauthorized;

        const creds = node.credentials || {};
        if (creds.sslCa) adapterConfig.sslCa = creds.sslCa;
        if (creds.sslCert) adapterConfig.sslCert = creds.sslCert;
        if (creds.sslKey) adapterConfig.sslKey = creds.sslKey;
        if (creds.sslPassphrase) adapterConfig.sslPassphrase = creds.sslPassphrase;
      }

      // SASL configuration
      if (node.authType.startsWith('sasl-')) {
        const creds = node.credentials || {};
        const mechanism = {
          'sasl-plain': 'plain',
          'sasl-scram-256': 'scram-sha-256',
          'sasl-scram-512': 'scram-sha-512',
          'sasl-oauthbearer': 'oauthbearer'
        }[node.authType];

        adapterConfig.sasl = { mechanism };

        if (['plain', 'scram-sha-256', 'scram-sha-512'].includes(mechanism)) {
          adapterConfig.sasl.username = creds.username || '';
          adapterConfig.sasl.password = creds.password || '';
        }
      }

      return adapterConfig;
    };

    /**
     * Connect to Kafka
     */
    node.doConnect = async function () {
      if (node.connected || node.connecting) return;
      if (node.invalidBrokers.length > 0) {
        node.setAllStatus({ fill: 'red', shape: 'ring', text: 'invalid broker URL' });
        node.error('Invalid broker URL(s): ' + node.invalidBrokers.join(', ') +
          '. Expected host:port (or PROTOCOL://host:port).');
        return;
      }
      node.connecting = true;
      node.setAllStatus({ fill: 'yellow', shape: 'ring', text: 'connecting...' });

      try {
        const adapterConfig = node.getAdapterConfig();
        node.client = createClient(node.backend, adapterConfig);
        // Forward disconnect/error events from the underlying client so we
        // notice when a previously healthy connection drops.
        if (typeof node.client.on === 'function') {
          node.client.on('disconnected', () => node._handleClientLost('disconnected'));
        }
        await node.client.connect();
        node.connected = true;
        node.connecting = false;
        node.setAllStatus({ fill: 'green', shape: 'dot', text: 'connected' });
        node.log('Connected to Kafka brokers: ' + node.brokers.join(', '));
      } catch (err) {
        node.connected = false;
        node.connecting = false;
        node.setAllStatus({ fill: 'red', shape: 'ring', text: 'error: ' + err.message });
        node.error('Failed to connect to Kafka: ' + err.message);
        // Schedule reconnect
        node._scheduleReconnect();
      }
    };

    /**
     * Schedule a reconnect attempt with capped exponential-style backoff
     * (delegates the actual interval growth to the underlying adapter via
     * initialRetryTime; we just keep retrying on failure).
     */
    node._scheduleReconnect = function () {
      if (node._reconnectTimer) clearTimeout(node._reconnectTimer);
      node._reconnectTimer = setTimeout(() => {
        node._reconnectTimer = null;
        if (!node.closing && Object.keys(node.users).length > 0) {
          node.doConnect();
        }
      }, node.initialRetryTime);
    };

    /**
     * Called when an established Kafka connection drops unexpectedly.
     */
    node._handleClientLost = function (reason) {
      if (node.closing || !node.connected) return;
      node.connected = false;
      node.client = null;
      node.setAllStatus({ fill: 'yellow', shape: 'ring', text: 'reconnecting (' + reason + ')' });
      node.warn('Kafka connection lost (' + reason + '), scheduling reconnect');
      node._scheduleReconnect();
    };

    /**
     * Disconnect from Kafka
     */
    node.doDisconnect = async function (done) {
      node.closing = true;
      if (node._reconnectTimer) {
        clearTimeout(node._reconnectTimer);
        node._reconnectTimer = null;
      }
      if (node.client) {
        try {
          await node.client.disconnect();
        } catch (err) {
          node.warn('Error disconnecting from Kafka: ' + err.message);
        }
        node.client = null;
      }
      node.connected = false;
      node.connecting = false;
      node.closing = false;
      node.setAllStatus({});
      if (done) done();
    };

    /**
     * Register a child node (producer/consumer/admin)
     */
    node.register = function (childNode) {
      node.users[childNode.id] = childNode;
      if (Object.keys(node.users).length === 1) {
        node.doConnect();
      } else if (node.connected) {
        childNode.status({ fill: 'green', shape: 'dot', text: 'connected' });
      } else if (node.connecting) {
        childNode.status({ fill: 'yellow', shape: 'ring', text: 'connecting...' });
      }
    };

    /**
     * Deregister a child node
     */
    node.deregister = function (childNode, done, autoDisconnect) {
      delete node.users[childNode.id];
      if (autoDisconnect && Object.keys(node.users).length === 0) {
        node.doDisconnect(done);
      } else {
        if (done) done();
      }
    };

    /**
     * Set status on all registered child nodes
     */
    node.setAllStatus = function (status) {
      for (const id in node.users) {
        if (node.users.hasOwnProperty(id)) {
          node.users[id].status(status);
        }
      }
    };

    /**
     * Get the Kafka client (adapter instance)
     */
    node.getClient = function () {
      return node.client;
    };

    // Close handler
    node.on('close', function (removed, done) {
      node.doDisconnect(done);
    });
  }

  RED.nodes.registerType('kafka-suite-broker', KafkaBrokerNode, {
    credentials: {
      username: { type: 'text' },
      password: { type: 'password' },
      sslCa: { type: 'password' },
      sslCert: { type: 'password' },
      sslKey: { type: 'password' },
      sslPassphrase: { type: 'password' }
    }
  });
};
