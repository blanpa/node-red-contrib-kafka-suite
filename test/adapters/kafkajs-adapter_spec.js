'use strict';

const should = require('should');
const KafkaJSAdapter = require('../../lib/adapters/kafkajs-adapter');
const {
  KafkaAdapter,
  ProducerAdapter,
  ConsumerAdapter,
  AdminAdapter
} = require('../../lib/adapter-interface');

describe('KafkaJSAdapter', function () {
  it('extends KafkaAdapter', function () {
    const a = new KafkaJSAdapter({ brokers: ['localhost:9092'] });
    a.should.be.instanceOf(KafkaAdapter);
  });

  it('createProducer throws before connect', function () {
    const a = new KafkaJSAdapter({ brokers: ['localhost:9092'] });
    (() => a.createProducer()).should.throw(/not connected/);
  });

  it('factories return correct adapter types when Kafka client is set', function () {
    const a = new KafkaJSAdapter({ brokers: ['localhost:9092'] });
    // Simulate post-connect state without network (connect() verifies brokers).
    a.kafka = new (require('kafkajs').Kafka)({
      brokers: ['localhost:9092'], clientId: 'test'
    });
    a.createProducer().should.be.instanceOf(ProducerAdapter);
    a.createConsumer('g').should.be.instanceOf(ConsumerAdapter);
    a.createAdmin().should.be.instanceOf(AdminAdapter);
  });

  describe('_buildConfig', function () {
    it('maps logLevel string → kafkajs enum', function () {
      const a = new KafkaJSAdapter({ brokers: ['b:9092'], logLevel: 'error' });
      const cfg = a._buildConfig();
      cfg.logLevel.should.be.a.Number();
    });

    it('includes retry settings', function () {
      const a = new KafkaJSAdapter({
        brokers: ['b:9092'],
        retries: 7,
        initialRetryTime: 100,
        maxRetryTime: 10000
      });
      const cfg = a._buildConfig();
      cfg.retry.retries.should.equal(7);
      cfg.retry.initialRetryTime.should.equal(100);
      cfg.retry.maxRetryTime.should.equal(10000);
    });

    it('omits ssl/sasl when not configured', function () {
      const a = new KafkaJSAdapter({ brokers: ['b:9092'] });
      const cfg = a._buildConfig();
      should(cfg.ssl).be.undefined();
      should(cfg.sasl).be.undefined();
    });

    it('builds SASL/PLAIN config', function () {
      const a = new KafkaJSAdapter({
        brokers: ['b:9092'],
        ssl: true,
        sasl: { mechanism: 'plain', username: 'u', password: 'p' }
      });
      const cfg = a._buildConfig();
      cfg.sasl.mechanism.should.equal('plain');
      cfg.sasl.username.should.equal('u');
      cfg.sasl.password.should.equal('p');
      cfg.ssl.should.equal(true);
    });

    it('builds SASL/SCRAM-SHA-512 config', function () {
      const a = new KafkaJSAdapter({
        brokers: ['b:9092'],
        ssl: true,
        sasl: { mechanism: 'scram-sha-512', username: 'u', password: 'p' }
      });
      const cfg = a._buildConfig();
      cfg.sasl.mechanism.should.equal('scram-sha-512');
      cfg.sasl.username.should.equal('u');
    });

    it('builds SSL config with CA/cert/key', function () {
      const a = new KafkaJSAdapter({
        brokers: ['b:9092'],
        ssl: 'verify',
        sslCa: 'CA',
        sslCert: 'CERT',
        sslKey: 'KEY',
        sslRejectUnauthorized: false
      });
      const cfg = a._buildConfig();
      cfg.ssl.ca.should.deepEqual(['CA']);
      cfg.ssl.cert.should.equal('CERT');
      cfg.ssl.key.should.equal('KEY');
      cfg.ssl.rejectUnauthorized.should.equal(false);
    });
  });

  describe('consumer lifecycle guards', function () {
    let adapter, consumer;

    beforeEach(function () {
      adapter = new KafkaJSAdapter({ brokers: ['localhost:9092'] });
      adapter.kafka = new (require('kafkajs').Kafka)({
        brokers: ['localhost:9092'], clientId: 'test'
      });
      consumer = adapter.createConsumer('g1');
    });

    it('pause() throws when called before run()', function () {
      (() => consumer.pause(['t'])).should.throw(/not running/);
    });

    it('resume() throws when called before run()', function () {
      (() => consumer.resume(['t'])).should.throw(/not running/);
    });

    it('seek() throws when called before run()', function () {
      return consumer.seek({ topic: 't', partition: 0, offset: 1 })
        .then(() => { throw new Error('should have thrown'); })
        .catch(err => { err.message.should.match(/not running/); });
    });
  });

  describe('producer serialization', function () {
    let adapter, producer;

    beforeEach(function () {
      adapter = new KafkaJSAdapter({ brokers: ['localhost:9092'] });
      adapter.kafka = new (require('kafkajs').Kafka)({
        brokers: ['localhost:9092'], clientId: 'test'
      });
      producer = adapter.createProducer();
    });

    it('_serializeValue: passes buffers through', function () {
      const b = Buffer.from('hi');
      producer._serializeValue(b).should.equal(b);
    });

    it('_serializeValue: JSON-stringifies objects', function () {
      producer._serializeValue({ a: 1 }).should.equal('{"a":1}');
    });

    it('_serializeValue: passes strings through', function () {
      producer._serializeValue('hi').should.equal('hi');
    });

    it('_serializeValue: null/undefined → null', function () {
      should(producer._serializeValue(null)).equal(null);
      should(producer._serializeValue(undefined)).equal(null);
    });

    it('_serializeValue: numbers → string', function () {
      producer._serializeValue(42).should.equal('42');
    });
  });
});
