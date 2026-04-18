'use strict';

const { Kafka, CompressionTypes, logLevel: KafkaLogLevel } = require('kafkajs');
const { KafkaAdapter, ProducerAdapter, ConsumerAdapter, AdminAdapter } = require('../adapter-interface');

const COMPRESSION_MAP = {
  none: CompressionTypes.None,
  gzip: CompressionTypes.GZIP,
  snappy: CompressionTypes.Snappy,
  lz4: CompressionTypes.LZ4,
  zstd: CompressionTypes.ZSTD
};

const LOG_LEVEL_MAP = {
  nothing: KafkaLogLevel.NOTHING,
  error: KafkaLogLevel.ERROR,
  warn: KafkaLogLevel.WARN,
  info: KafkaLogLevel.INFO,
  debug: KafkaLogLevel.DEBUG
};

class KafkaJSAdapter extends KafkaAdapter {
  constructor(config) {
    super(config);
    this.kafka = null;
    this._connected = false;
  }

  async connect() {
    const kafkaConfig = this._buildConfig();
    this.kafka = new Kafka(kafkaConfig);
    // Verify reachability with a short-lived admin so status reflects real broker contact.
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      await admin.describeCluster();
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
    return new KafkaJSProducerAdapter(this.kafka, options);
  }

  createConsumer(groupId, options = {}) {
    if (!this.kafka) throw new Error('Kafka client not connected. Call connect() first.');
    return new KafkaJSConsumerAdapter(this.kafka, groupId, options);
  }

  createAdmin() {
    if (!this.kafka) throw new Error('Kafka client not connected. Call connect() first.');
    return new KafkaJSAdminAdapter(this.kafka);
  }

  _buildConfig() {
    const config = {
      clientId: this.config.clientId || 'node-red-kafka-suite',
      brokers: this.config.brokers,
      connectionTimeout: this.config.connectionTimeout || 3000,
      requestTimeout: this.config.requestTimeout || 30000,
      logLevel: LOG_LEVEL_MAP[this.config.logLevel] || KafkaLogLevel.WARN,
      retry: {
        maxRetryTime: this.config.maxRetryTime || 30000,
        initialRetryTime: this.config.initialRetryTime || 300,
        retries: this.config.retries || 5,
        factor: 0.2,
        multiplier: 2
      }
    };

    // SSL/TLS
    if (this.config.ssl) {
      config.ssl = this._buildSSLConfig();
    }

    // SASL
    if (this.config.sasl) {
      config.sasl = this._buildSASLConfig();
    }

    return config;
  }

  _buildSSLConfig() {
    const ssl = {};
    if (this.config.ssl === true) return true;

    if (this.config.sslCa) ssl.ca = [this.config.sslCa];
    if (this.config.sslCert) ssl.cert = this.config.sslCert;
    if (this.config.sslKey) ssl.key = this.config.sslKey;
    if (this.config.sslPassphrase) ssl.passphrase = this.config.sslPassphrase;
    if (this.config.sslRejectUnauthorized !== undefined) {
      ssl.rejectUnauthorized = this.config.sslRejectUnauthorized;
    }

    return Object.keys(ssl).length > 0 ? ssl : true;
  }

  _buildSASLConfig() {
    const sasl = {
      mechanism: this.config.sasl.mechanism
    };

    switch (this.config.sasl.mechanism) {
      case 'plain':
      case 'scram-sha-256':
      case 'scram-sha-512':
        sasl.username = this.config.sasl.username;
        sasl.password = this.config.sasl.password;
        break;
      case 'oauthbearer':
        sasl.oauthBearerProvider = this.config.sasl.oauthBearerProvider;
        break;
      case 'aws':
        sasl.authorizationIdentity = this.config.sasl.authorizationIdentity;
        sasl.accessKeyId = this.config.sasl.accessKeyId;
        sasl.secretAccessKey = this.config.sasl.secretAccessKey;
        sasl.sessionToken = this.config.sasl.sessionToken;
        break;
    }

    return sasl;
  }
}

class KafkaJSProducerAdapter extends ProducerAdapter {
  constructor(kafka, options) {
    super();
    this.producer = kafka.producer({
      allowAutoTopicCreation: options.allowAutoTopicCreation !== false,
      idempotent: options.idempotent || false,
      maxInFlightRequests: options.maxInFlightRequests || undefined
    });
  }

  async connect() {
    await this.producer.connect();
  }

  async send({ topic, messages, acks, timeout, compression }) {
    const result = await this.producer.send({
      topic,
      messages: messages.map(m => ({
        key: m.key != null ? String(m.key) : null,
        value: this._serializeValue(m.value),
        headers: m.headers || undefined,
        partition: m.partition != null ? Number(m.partition) : undefined,
        timestamp: m.timestamp || undefined
      })),
      acks: acks != null ? Number(acks) : -1,
      timeout: timeout || 30000,
      compression: COMPRESSION_MAP[compression] || CompressionTypes.None
    });
    return result;
  }

  async sendBatch({ topicMessages, acks, timeout, compression }) {
    const result = await this.producer.sendBatch({
      topicMessages: topicMessages.map(tm => ({
        topic: tm.topic,
        messages: tm.messages.map(m => ({
          key: m.key != null ? String(m.key) : null,
          value: this._serializeValue(m.value),
          headers: m.headers || undefined,
          partition: m.partition != null ? Number(m.partition) : undefined
        }))
      })),
      acks: acks != null ? Number(acks) : -1,
      timeout: timeout || 30000,
      compression: COMPRESSION_MAP[compression] || CompressionTypes.None
    });
    return result;
  }

  async disconnect() {
    await this.producer.disconnect();
  }

  _serializeValue(value) {
    if (value === null || value === undefined) return null;
    if (Buffer.isBuffer(value)) return value;
    if (typeof value === 'string') return value;
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }
}

class KafkaJSConsumerAdapter extends ConsumerAdapter {
  constructor(kafka, groupId, options) {
    super();
    this._eventHandlers = {};
    this._running = false;
    this.consumer = kafka.consumer({
      groupId,
      sessionTimeout: options.sessionTimeout || 30000,
      heartbeatInterval: options.heartbeatInterval || 3000,
      maxBytesPerPartition: options.maxBytesPerPartition || 1048576,
      maxWaitTimeInMs: options.maxWaitTimeInMs || 5000,
      partitionAssigners: undefined,
      allowAutoTopicCreation: options.allowAutoTopicCreation !== false
    });

    // Forward rebalance events
    const { CONNECT, DISCONNECT, GROUP_JOIN, REBALANCING, STOP } = this.consumer.events;
    this.consumer.on(GROUP_JOIN, (e) => this._emit('ready', e));
    this.consumer.on(REBALANCING, (e) => this._emit('rebalancing', e));
    this.consumer.on(CONNECT, (e) => this._emit('connected', e));
    this.consumer.on(DISCONNECT, (e) => this._emit('disconnected', e));
    this.consumer.on(STOP, (e) => this._emit('stopped', e));
  }

  on(event, handler) {
    if (!this._eventHandlers[event]) {
      this._eventHandlers[event] = [];
    }
    this._eventHandlers[event].push(handler);
  }

  _emit(event, ...args) {
    const handlers = this._eventHandlers[event] || [];
    handlers.forEach(h => h(...args));
  }

  async connect() {
    await this.consumer.connect();
  }

  async subscribe({ topics, fromBeginning }) {
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: fromBeginning || false });
    }
  }

  async run({ eachMessage, autoCommit, autoCommitInterval, partitionsConsumedConcurrently }) {
    await this.consumer.run({
      eachMessage,
      autoCommit: autoCommit !== false,
      autoCommitInterval: autoCommitInterval || 5000,
      partitionsConsumedConcurrently: partitionsConsumedConcurrently || 1
    });
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
    await this.consumer.disconnect();
  }
}

class KafkaJSAdminAdapter extends AdminAdapter {
  constructor(kafka) {
    super();
    this.admin = kafka.admin();
  }

  async connect() {
    await this.admin.connect();
  }

  async listTopics() {
    return await this.admin.listTopics();
  }

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

  async deleteTopics(topics) {
    return await this.admin.deleteTopics({ topics });
  }

  async describeCluster() {
    return await this.admin.describeCluster();
  }

  async listGroups() {
    return await this.admin.listGroups();
  }

  async describeGroups(groupIds) {
    return await this.admin.describeGroups(groupIds);
  }

  async fetchTopicOffsets(topic) {
    return await this.admin.fetchTopicOffsets(topic);
  }

  async resetOffsets({ groupId, topic, earliest }) {
    return await this.admin.resetOffsets({
      groupId,
      topic,
      earliest: earliest !== false
    });
  }

  async deleteGroups(groupIds) {
    return await this.admin.deleteGroups(groupIds);
  }

  async disconnect() {
    await this.admin.disconnect();
  }
}

module.exports = KafkaJSAdapter;
