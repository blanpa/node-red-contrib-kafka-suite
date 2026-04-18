'use strict';

const { KafkaAdapter, ProducerAdapter, ConsumerAdapter, AdminAdapter } = require('../adapter-interface');

let ConfluentKafka;
try {
  const pkg = require('@confluentinc/kafka-javascript');
  ConfluentKafka = pkg.KafkaJS;
} catch (e) {
  // Will throw when trying to use this adapter without the package installed
}

class ConfluentAdapter extends KafkaAdapter {
  constructor(config) {
    super(config);
    if (!ConfluentKafka) {
      throw new Error(
        'Confluent adapter requires @confluentinc/kafka-javascript. ' +
        'Install it with: npm install @confluentinc/kafka-javascript'
      );
    }
    this.kafka = null;
    this._connected = false;
  }

  async connect() {
    const kafkaConfig = this._buildConfig();
    this.kafka = new ConfluentKafka.Kafka(kafkaConfig);
    // Verify reachability with a short-lived admin so connection failures
    // surface here instead of later when the first producer/consumer is used.
    // NOTE: @confluentinc/kafka-javascript's KafkaJS-compat admin does NOT
    // expose describeCluster() (only listTopics/createTopics/etc). We use
    // listTopics() which is metadata-only and cheap.
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      await admin.listTopics();
    } catch (err) {
      this.kafka = null;
      throw err;
    } finally {
      try { await admin.disconnect(); } catch (e) { /* ignore */ }
    }
    this._connected = true;
    this.emit('connected');
  }

  async disconnect() {
    this._connected = false;
    this.kafka = null;
    this.emit('disconnected');
  }

  isConnected() {
    return this._connected;
  }

  createProducer(options = {}) {
    if (!this.kafka) throw new Error('Kafka client not connected. Call connect() first.');
    return new ConfluentProducerAdapter(this.kafka, options);
  }

  createConsumer(groupId, options = {}) {
    if (!this.kafka) throw new Error('Kafka client not connected. Call connect() first.');
    return new ConfluentConsumerAdapter(this.kafka, groupId, options);
  }

  createAdmin() {
    if (!this.kafka) throw new Error('Kafka client not connected. Call connect() first.');
    return new ConfluentAdminAdapter(this.kafka);
  }

  _buildConfig() {
    // Confluent's KafkaJS-compatible API splits the configuration:
    //   - KafkaJS-style high-level options go into a `kafkaJS` sub-block.
    //   - librdkafka native properties (ssl.ca.pem, sasl.username, ...) live
    //     at the *root* of the config object. Mixing them (e.g. setting
    //     `ssl: { ca: ... }` inside `kafkaJS`) raises:
    //       "The 'ssl' property must be a boolean. Any additional
    //        configuration must be provided outside the kafkaJS block."
    const kafkaJS = {
      clientId: this.config.clientId || 'node-red-kafka-suite',
      brokers: this.config.brokers,
      connectionTimeout: this.config.connectionTimeout || 3000,
      requestTimeout: this.config.requestTimeout || 30000
    };

    const root = {};

    if (this.config.ssl) {
      // boolean toggle stays inside kafkaJS
      kafkaJS.ssl = true;
      // PEM material -> librdkafka properties
      if (this.config.sslCa) root['ssl.ca.pem'] = this.config.sslCa;
      if (this.config.sslCert) root['ssl.certificate.pem'] = this.config.sslCert;
      if (this.config.sslKey) root['ssl.key.pem'] = this.config.sslKey;
      if (this.config.sslPassphrase) root['ssl.key.password'] = this.config.sslPassphrase;
      if (this.config.sslRejectUnauthorized === false) {
        root['enable.ssl.certificate.verification'] = false;
      }
    }

    if (this.config.sasl) {
      // librdkafka picks up sasl via these root keys
      const mech = String(this.config.sasl.mechanism || '').toUpperCase();
      // librdkafka uses uppercase mechanism identifiers
      // "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"
      root['sasl.mechanism'] = mech;
      // protocol must be SASL_PLAINTEXT or SASL_SSL depending on whether ssl is on
      root['security.protocol'] = this.config.ssl ? 'SASL_SSL' : 'SASL_PLAINTEXT';
      if (this.config.sasl.username) root['sasl.username'] = this.config.sasl.username;
      if (this.config.sasl.password) root['sasl.password'] = this.config.sasl.password;
    } else if (this.config.ssl) {
      // SSL only without SASL -> security.protocol = SSL
      root['security.protocol'] = 'SSL';
    }

    return { kafkaJS, ...root };
  }
}

class ConfluentProducerAdapter extends ProducerAdapter {
  constructor(kafka, options) {
    super();
    this._kafka = kafka;
    this._options = options || {};
    this.producer = null;
    this._connected = false;
  }

  _ensureProducer({ acks, timeout, compression } = {}) {
    // @confluentinc/kafka-javascript requires send-options like acks/timeout
    // to be set at producer creation, not on send(). Defer until we know them.
    if (this.producer) return;
    const kafkaJS = { ...this._options };
    if (acks != null) kafkaJS.acks = Number(acks);
    if (timeout != null) kafkaJS.timeout = timeout;
    if (compression && compression !== 'none') kafkaJS.compression = compression;
    this.producer = this._kafka.producer({ kafkaJS });
  }

  async connect() {
    // Defer real connect until first send so we can pass acks/timeout/compression
    this._connected = true;
  }

  async _connectIfNeeded() {
    if (this.producer && !this.producer._actuallyConnected) {
      await this.producer.connect();
      this.producer._actuallyConnected = true;
    }
  }

  async send({ topic, messages, acks, timeout, compression }) {
    this._ensureProducer({ acks, timeout, compression });
    await this._connectIfNeeded();
    return await this.producer.send({
      topic,
      messages: messages.map(m => ({
        key: m.key != null ? String(m.key) : null,
        value: this._serializeValue(m.value),
        headers: m.headers || undefined,
        partition: m.partition != null ? Number(m.partition) : undefined
      }))
    });
  }

  async sendBatch({ topicMessages, acks, timeout, compression }) {
    this._ensureProducer({ acks, timeout, compression });
    await this._connectIfNeeded();
    return await this.producer.sendBatch({ topicMessages });
  }

  async disconnect() {
    this._connected = false;
    if (this.producer && this.producer._actuallyConnected) {
      await this.producer.disconnect();
      this.producer._actuallyConnected = false;
    }
    this.producer = null;
  }

  _serializeValue(value) {
    if (value === null || value === undefined) return null;
    if (Buffer.isBuffer(value)) return value;
    if (typeof value === 'string') return value;
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }
}

class ConfluentConsumerAdapter extends ConsumerAdapter {
  constructor(kafka, groupId, options) {
    super();
    this._eventHandlers = {};
    this._kafka = kafka;
    this._groupId = groupId;
    this._consumerOptions = options || {};
    this.consumer = null;
    this._running = false;
  }

  on(event, handler) {
    if (!this._eventHandlers[event]) this._eventHandlers[event] = [];
    this._eventHandlers[event].push(handler);
  }

  _emit(event, ...args) {
    (this._eventHandlers[event] || []).forEach(h => h(...args));
  }

  _ensureConsumer() {
    // @confluentinc/kafka-javascript requires fromBeginning, autoCommit, etc.
    // to be set at consumer creation, not on subscribe/run. Defer creation
    // until we know all the options.
    if (!this.consumer) {
      this.consumer = this._kafka.consumer({
        kafkaJS: {
          groupId: this._groupId,
          ...this._consumerOptions,
          ...this._deferredOptions
        }
      });
    }
  }

  async connect() {
    // Defer real connect until subscribe/run, because we need all options first
    this._deferredOptions = {};
  }

  async subscribe({ topics, fromBeginning }) {
    if (fromBeginning !== undefined) this._deferredOptions.fromBeginning = fromBeginning;
    this._pendingTopics = topics;
  }

  async run({ eachMessage, autoCommit, autoCommitInterval, partitionsConsumedConcurrently }) {
    if (autoCommit !== undefined) this._deferredOptions.autoCommit = autoCommit;
    if (autoCommitInterval !== undefined) this._deferredOptions.autoCommitInterval = autoCommitInterval;
    this._ensureConsumer();
    await this.consumer.connect();
    for (const topic of this._pendingTopics) {
      await this.consumer.subscribe({ topic });
    }
    await this.consumer.run({ eachMessage });
    this._running = true;
  }

  async commitOffsets(offsets) {
    await this.consumer.commitOffsets(offsets);
  }

  pause(topics) {
    if (!this._running) throw new Error('Consumer is not running yet — call run() first');
    this.consumer.pause(topics.map(t => ({ topic: t })));
  }

  resume(topics) {
    if (!this._running) throw new Error('Consumer is not running yet — call run() first');
    this.consumer.resume(topics.map(t => ({ topic: t })));
  }

  async seek({ topic, partition, offset }) {
    if (!this._running) throw new Error('Consumer is not running yet — call run() first');
    this.consumer.seek({ topic, partition, offset: String(offset) });
  }

  async disconnect() {
    this._running = false;
    if (this.consumer) await this.consumer.disconnect();
  }
}

class ConfluentAdminAdapter extends AdminAdapter {
  constructor(kafka) {
    super();
    this.admin = kafka.admin();
  }

  async connect() { await this.admin.connect(); }
  async listTopics() { return await this.admin.listTopics(); }
  async createTopics(topics) {
    return await this.admin.createTopics({
      topics: topics.map(t => ({
        topic: t.topic,
        numPartitions: t.numPartitions || 1,
        replicationFactor: t.replicationFactor || 1,
        configEntries: t.configEntries || []
      }))
    });
  }
  async deleteTopics(topics) { return await this.admin.deleteTopics({ topics }); }
  async describeCluster() { return await this.admin.describeCluster(); }
  async listGroups() { return await this.admin.listGroups(); }
  async describeGroups(groupIds) { return await this.admin.describeGroups(groupIds); }
  async fetchTopicOffsets(topic) { return await this.admin.fetchTopicOffsets(topic); }
  async resetOffsets({ groupId, topic, earliest }) {
    return await this.admin.resetOffsets({ groupId, topic, earliest: earliest !== false });
  }
  async deleteGroups(groupIds) { return await this.admin.deleteGroups(groupIds); }
  async disconnect() { await this.admin.disconnect(); }
}

module.exports = ConfluentAdapter;
