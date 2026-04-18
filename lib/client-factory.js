'use strict';

const KafkaJSAdapter = require('./adapters/kafkajs-adapter');

function createClient(backend, config) {
  if (backend === 'confluent') {
    try {
      const ConfluentAdapter = require('./adapters/confluent-adapter');
      return new ConfluentAdapter(config);
    } catch (e) {
      if (e.message && e.message.includes('@confluentinc/kafka-javascript')) {
        throw e;
      }
      throw new Error(
        'Confluent adapter requires @confluentinc/kafka-javascript. ' +
        'Install it with: npm install @confluentinc/kafka-javascript'
      );
    }
  }
  return new KafkaJSAdapter(config);
}

module.exports = { createClient };
