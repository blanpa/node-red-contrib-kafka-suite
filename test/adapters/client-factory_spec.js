'use strict';

const should = require('should');
const { createClient } = require('../../lib/client-factory');
const KafkaJSAdapter = require('../../lib/adapters/kafkajs-adapter');

let confluentAvailable = true;
try { require('@confluentinc/kafka-javascript'); } catch (e) { confluentAvailable = false; }

describe('client-factory', function () {
  it('returns KafkaJSAdapter for backend=kafkajs', function () {
    const c = createClient('kafkajs', { brokers: ['localhost:9092'] });
    c.should.be.instanceOf(KafkaJSAdapter);
  });

  it('defaults to KafkaJSAdapter when backend is unknown', function () {
    const c = createClient('unknown', { brokers: ['localhost:9092'] });
    c.should.be.instanceOf(KafkaJSAdapter);
  });

  if (confluentAvailable) {
    it('returns ConfluentAdapter when backend=confluent and package is installed', function () {
      const ConfluentAdapter = require('../../lib/adapters/confluent-adapter');
      const c = createClient('confluent', { brokers: ['localhost:9092'] });
      c.should.be.instanceOf(ConfluentAdapter);
    });
  } else {
    it('throws descriptive error for confluent backend without package', function () {
      (() => createClient('confluent', { brokers: ['localhost:9092'] }))
        .should.throw(/@confluentinc\/kafka-javascript/);
    });
  }
});
