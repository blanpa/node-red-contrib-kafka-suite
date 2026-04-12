'use strict';

/**
 * Base adapter interface that both kafkajs and confluent adapters must implement.
 * This defines the contract for the abstraction layer.
 */

class KafkaAdapter {
  constructor(config) {
    this.config = config;
    this._eventHandlers = {};
  }

  async connect() {
    throw new Error('connect() must be implemented by adapter');
  }

  async disconnect() {
    throw new Error('disconnect() must be implemented by adapter');
  }

  createProducer(options) {
    throw new Error('createProducer() must be implemented by adapter');
  }

  createConsumer(groupId, options) {
    throw new Error('createConsumer() must be implemented by adapter');
  }

  createAdmin() {
    throw new Error('createAdmin() must be implemented by adapter');
  }

  on(event, handler) {
    if (!this._eventHandlers[event]) {
      this._eventHandlers[event] = [];
    }
    this._eventHandlers[event].push(handler);
  }

  emit(event, ...args) {
    const handlers = this._eventHandlers[event] || [];
    handlers.forEach(h => h(...args));
  }
}

class ProducerAdapter {
  async connect() {
    throw new Error('connect() must be implemented by adapter');
  }

  async send({ topic, messages, acks, timeout, compression }) {
    throw new Error('send() must be implemented by adapter');
  }

  async sendBatch({ topicMessages, acks, timeout, compression }) {
    throw new Error('sendBatch() must be implemented by adapter');
  }

  async disconnect() {
    throw new Error('disconnect() must be implemented by adapter');
  }
}

class ConsumerAdapter {
  async connect() {
    throw new Error('connect() must be implemented by adapter');
  }

  async subscribe({ topics, fromBeginning }) {
    throw new Error('subscribe() must be implemented by adapter');
  }

  async run({ eachMessage, autoCommit, autoCommitInterval }) {
    throw new Error('run() must be implemented by adapter');
  }

  async commitOffsets(offsets) {
    throw new Error('commitOffsets() must be implemented by adapter');
  }

  pause(topics) {
    throw new Error('pause() must be implemented by adapter');
  }

  resume(topics) {
    throw new Error('resume() must be implemented by adapter');
  }

  async seek({ topic, partition, offset }) {
    throw new Error('seek() must be implemented by adapter');
  }

  async disconnect() {
    throw new Error('disconnect() must be implemented by adapter');
  }

  on(event, handler) {
    // Adapters should implement event handling for: 'rebalancing', 'ready'
  }
}

class AdminAdapter {
  async connect() {
    throw new Error('connect() must be implemented by adapter');
  }

  async listTopics() {
    throw new Error('listTopics() must be implemented by adapter');
  }

  async createTopics(topics) {
    throw new Error('createTopics() must be implemented by adapter');
  }

  async deleteTopics(topics) {
    throw new Error('deleteTopics() must be implemented by adapter');
  }

  async describeCluster() {
    throw new Error('describeCluster() must be implemented by adapter');
  }

  async listGroups() {
    throw new Error('listGroups() must be implemented by adapter');
  }

  async describeGroups(groupIds) {
    throw new Error('describeGroups() must be implemented by adapter');
  }

  async fetchTopicOffsets(topic) {
    throw new Error('fetchTopicOffsets() must be implemented by adapter');
  }

  async resetOffsets({ groupId, topic, earliest }) {
    throw new Error('resetOffsets() must be implemented by adapter');
  }

  async deleteGroups(groupIds) {
    throw new Error('deleteGroups() must be implemented by adapter');
  }

  async disconnect() {
    throw new Error('disconnect() must be implemented by adapter');
  }
}

module.exports = { KafkaAdapter, ProducerAdapter, ConsumerAdapter, AdminAdapter };
