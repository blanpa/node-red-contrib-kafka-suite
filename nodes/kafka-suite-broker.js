'use strict';

const { createClient } = require('../lib/client-factory');
const { intOr, normalizeBroker, Backoff, formatDelay } = require('../lib/util');

module.exports = function (RED) {
  function KafkaBrokerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Config properties
    node.name = config.name;
    const rawBrokers = (config.brokers || 'localhost:9092').split(',').map(b => b.trim()).filter(Boolean);
    node.clientId = config.clientId || 'node-red-kafka-suite';

    // Accepted forms: host:port or PROTOCOL://host:port (e.g.
    // SASL_SSL://kafka.example.com:9094). The protocol prefix is stripped
    // because neither client library understands it; security settings come
    // from the auth configuration.
    node.brokers = rawBrokers.map(normalizeBroker).filter(Boolean);
    node.invalidBrokers = rawBrokers.filter(b => !normalizeBroker(b));
    node.backend = config.backend || 'kafkajs';
    node.servicePreset = config.servicePreset || 'self-hosted';
    node.authType = config.authType || 'none';
    // TLS for SASL mechanisms. Defaults to on (configs saved before this
    // option existed always used TLS); off means SASL_PLAINTEXT.
    node.saslSsl = config.saslSsl !== false;
    // OAuth 2.0 (SASL/OAUTHBEARER) non-secret settings
    node.oauthTokenEndpoint = config.oauthTokenEndpoint || '';
    node.oauthGrantType = config.oauthGrantType || 'client_credentials';
    node.oauthScope = config.oauthScope || '';
    node.oauthAudience = config.oauthAudience || '';
    // Verify TLS on the token endpoint unless explicitly disabled (checkbox).
    node.oauthTlsReject = config.oauthTlsReject !== false;
    node.logLevel = config.logLevel || 'warn';
    node.connectionTimeout = intOr(config.connectionTimeout, 3000, 1);
    node.requestTimeout = intOr(config.requestTimeout, 30000, 1);
    node.maxRetryTime = intOr(config.maxRetryTime, 30000, 0);
    node.initialRetryTime = intOr(config.initialRetryTime, 300, 1);
    node.retries = intOr(config.retries, 5, 0);
    node.sslRejectUnauthorized = config.sslRejectUnauthorized !== false;

    // User registry (MQTT-style pattern)
    node.users = {};
    node.client = null;
    node.connected = false;
    node.connecting = false;
    // Bumped by doDisconnect() so a connect attempt still in flight knows
    // its result is stale and must not be adopted.
    node._connectGen = 0;
    node._reconnectTimer = null;
    node._reconnectBackoff = new Backoff(node.initialRetryTime, Math.max(node.maxRetryTime, node.initialRetryTime));

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
      const isSasl = node.authType.startsWith('sasl-');
      const needsSSL = node.authType === 'ssl' || (isSasl && node.saslSsl);
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
      if (isSasl) {
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
        } else if (mechanism === 'oauthbearer') {
          // OAuth 2.0 (Strimzi-style) bearer token acquisition. The token
          // endpoint URL, grant type, scope and audience are plain config;
          // client id/secret and the password-grant username/password are
          // credentials. The adapters turn this into a token provider.
          adapterConfig.sasl.oauth = {
            tokenEndpoint: node.oauthTokenEndpoint,
            grantType: node.oauthGrantType,
            clientId: creds.oauthClientId || '',
            clientSecret: creds.oauthClientSecret || '',
            username: creds.oauthUsername || '',
            password: creds.oauthPassword || '',
            scope: node.oauthScope || '',
            audience: node.oauthAudience || '',
            // Defaults to verifying; only disabled when explicitly unchecked.
            rejectUnauthorized: node.oauthTlsReject
          };
        }
      }

      return adapterConfig;
    };

    /**
     * Connect to Kafka. The adapter's connect() is a reachability probe; on
     * success every registered child is told to start its producer/consumer/
     * admin. On failure we retry with exponential backoff (initialRetryTime,
     * doubling up to maxRetryTime).
     */
    node.doConnect = async function () {
      if (node.connected || node.connecting) return;
      if (node.invalidBrokers.length > 0) {
        node.setAllStatus({ fill: 'red', shape: 'ring', text: 'invalid broker URL' });
        // Every registering child calls doConnect(); report the config error once.
        if (node._invalidReported) return;
        node._invalidReported = true;
        node.error('Invalid broker URL(s): ' + node.invalidBrokers.join(', ') +
          '. Expected host:port (or PROTOCOL://host:port).');
        return;
      }
      const gen = node._connectGen;
      node.connecting = true;
      node.setAllStatus({ fill: 'yellow', shape: 'ring', text: 'connecting...' });

      let client = null;
      try {
        client = createClient(node.backend, node.getAdapterConfig());
        await client.connect();
        if (gen !== node._connectGen) {
          // Disconnected while we were connecting: drop this client.
          try { await client.disconnect(); } catch (e) { /* ignore */ }
          return;
        }
        node.client = client;
        node.connected = true;
        node.connecting = false;
        node._reconnectBackoff.reset();
        node.log('Connected to Kafka brokers: ' + node.brokers.join(', '));
        node._notifyUsers('_onBrokerConnected');
      } catch (err) {
        if (gen !== node._connectGen) return;
        node.connected = false;
        node.connecting = false;
        const delay = node._scheduleReconnect();
        node.setAllStatus({ fill: 'red', shape: 'ring', text: 'error: ' + err.message });
        node.error('Failed to connect to Kafka: ' + err.message +
          ' (retrying in ' + formatDelay(delay) + ')');
      }
    };

    /**
     * Schedule a reconnect attempt after the next backoff delay. Returns the
     * delay in ms.
     */
    node._scheduleReconnect = function () {
      if (node._reconnectTimer) clearTimeout(node._reconnectTimer);
      const delay = node._reconnectBackoff.next();
      node._reconnectTimer = setTimeout(() => {
        node._reconnectTimer = null;
        if (Object.keys(node.users).length > 0) {
          node.doConnect();
        }
      }, delay);
      return delay;
    };

    /**
     * Disconnect from Kafka
     */
    node.doDisconnect = async function (done) {
      node._connectGen++;
      if (node._reconnectTimer) {
        clearTimeout(node._reconnectTimer);
        node._reconnectTimer = null;
      }
      node._reconnectBackoff.reset();
      const client = node.client;
      node.client = null;
      node.connected = false;
      node.connecting = false;
      if (client) {
        try {
          await client.disconnect();
        } catch (err) {
          node.warn('Error disconnecting from Kafka: ' + err.message);
        }
      }
      node.setAllStatus({});
      if (done) done();
    };

    /**
     * Register a child node (producer/consumer/admin)
     */
    node.register = function (childNode) {
      node.users[childNode.id] = childNode;
      if (node.connected) {
        if (typeof childNode._onBrokerConnected === 'function') childNode._onBrokerConnected();
      } else if (node.connecting) {
        childNode.status({ fill: 'yellow', shape: 'ring', text: 'connecting...' });
      } else if (node._reconnectTimer) {
        childNode.status({ fill: 'yellow', shape: 'ring', text: 'waiting to reconnect...' });
      } else {
        node.doConnect();
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
        if (Object.prototype.hasOwnProperty.call(node.users, id)) {
          node.users[id].status(status);
        }
      }
    };

    /**
     * Call a lifecycle hook (e.g. _onBrokerConnected) on every child that
     * implements it.
     */
    node._notifyUsers = function (hook) {
      for (const id of Object.keys(node.users)) {
        const child = node.users[id];
        if (child && typeof child[hook] === 'function') child[hook]();
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
      sslPassphrase: { type: 'password' },
      oauthClientId: { type: 'text' },
      oauthClientSecret: { type: 'password' },
      oauthUsername: { type: 'text' },
      oauthPassword: { type: 'password' }
    }
  });
};
