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
    // Confluent's KafkaJS-compatible API uses the same config format
    const config = {
      clientId: this.config.clientId || 'node-red-kafka-suite',
      brokers: this.config.brokers,
      connectionTimeout: this.config.connectionTimeout || 3000,
      requestTimeout: this.config.requestTimeout || 30000
    };

    if (this.config.ssl) {
      config.ssl = this.config.ssl === true ? true : {
        ca: this.config.sslCa ? [this.config.sslCa] : undefined,
        cert: this.config.sslCert || undefined,
        key: this.config.sslKey || undefined,
        passphrase: this.config.sslPassphrase || undefined,
        rejectUnauthorized: this.config.sslRejectUnauthorized
      };
    }

    if (this.config.sasl) {
      config.sasl = {
        mechanism: this.config.sasl.mechanism,
        username: this.config.sasl.username,
        password: this.config.sasl.password
      };
    }

    return config;
  }
}

class ConfluentProducerAdapter extends ProducerAdapter {
  constructor(kafka, options) {
    super();
    this.producer = kafka.producer(options);
  }

  async connect() {
    await this.producer.connect();
  }

  async send({ topic, messages, acks, timeout, compression }) {
    return await this.producer.send({
      topic,
      messages: messages.map(m => ({
        key: m.key != null ? String(m.key) : null,
        value: this._serializeValue(m.value),
        headers: m.headers || undefined,
        partition: m.partition != null ? Number(m.partition) : undefined
      })),
      acks: acks != null ? Number(acks) : -1,
      timeout: timeout || 30000
    });
  }

  async sendBatch({ topicMessages, acks, timeout, compression }) {
    return await this.producer.sendBatch({
      topicMessages,
      acks: acks != null ? Number(acks) : -1,
      timeout: timeout || 30000
    });
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

class ConfluentConsumerAdapter extends ConsumerAdapter {
  constructor(kafka, groupId, options) {
    super();
    this._eventHandlers = {};
    this.consumer = kafka.consumer({ groupId, ...options });
  }

  on(event, handler) {
    if (!this._eventHandlers[event]) this._eventHandlers[event] = [];
    this._eventHandlers[event].push(handler);
  }

  _emit(event, ...args) {
    (this._eventHandlers[event] || []).forEach(h => h(...args));
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
      autoCommitInterval: autoCommitInterval || 5000
    });
  }

  async commitOffsets(offsets) {
    await this.consumer.commitOffsets(offsets);
  }

  pause(topics) {
    this.consumer.pause(topics.map(t => ({ topic: t })));
  }

  resume(topics) {
    this.consumer.resume(topics.map(t => ({ topic: t })));
  }

  async seek({ topic, partition, offset }) {
    this.consumer.seek({ topic, partition, offset: String(offset) });
  }

  async disconnect() {
    await this.consumer.disconnect();
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
