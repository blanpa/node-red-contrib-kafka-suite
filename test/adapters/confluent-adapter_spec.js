'use strict';

const should = require('should');

let confluentAvailable = true;
try { require('@confluentinc/kafka-javascript'); } catch (e) { confluentAvailable = false; }

const describeIfAvailable = confluentAvailable ? describe : describe.skip;

describeIfAvailable('ConfluentAdapter', function () {
  const ConfluentAdapter = require('../../lib/adapters/confluent-adapter');
  const {
    KafkaAdapter,
    ProducerAdapter,
    ConsumerAdapter,
    AdminAdapter
  } = require('../../lib/adapter-interface');

  it('extends KafkaAdapter', function () {
    const a = new ConfluentAdapter({ brokers: ['localhost:9092'] });
    a.should.be.instanceOf(KafkaAdapter);
  });

  it('createProducer/Consumer/Admin throw before connect', function () {
    const a = new ConfluentAdapter({ brokers: ['localhost:9092'] });
    (() => a.createProducer()).should.throw(/not connected/);
    (() => a.createConsumer('g')).should.throw(/not connected/);
    (() => a.createAdmin()).should.throw(/not connected/);
  });

  describe('_buildConfig', function () {
    it('wraps KafkaJS-style props in kafkaJS block', function () {
      const a = new ConfluentAdapter({
        brokers: ['b:9092'], clientId: 'cid', connectionTimeout: 5000
      });
      const cfg = a._buildConfig();
      cfg.should.have.property('kafkaJS');
      cfg.kafkaJS.clientId.should.equal('cid');
      cfg.kafkaJS.brokers.should.deepEqual(['b:9092']);
      cfg.kafkaJS.connectionTimeout.should.equal(5000);
    });

    it('uses defaults when not provided', function () {
      const a = new ConfluentAdapter({ brokers: ['b:9092'] });
      const cfg = a._buildConfig();
      cfg.kafkaJS.clientId.should.equal('node-red-kafka-suite');
      cfg.kafkaJS.connectionTimeout.should.equal(3000);
      cfg.kafkaJS.requestTimeout.should.equal(30000);
    });

    it('omits ssl/sasl when not configured', function () {
      const a = new ConfluentAdapter({ brokers: ['b:9092'] });
      const cfg = a._buildConfig();
      should(cfg.kafkaJS.ssl).be.undefined();
      should(cfg.kafkaJS.sasl).be.undefined();
    });

    it('includes ssl=true when ssl is true', function () {
      const a = new ConfluentAdapter({ brokers: ['b:9092'], ssl: true });
      const cfg = a._buildConfig();
      cfg.kafkaJS.ssl.should.equal(true);
    });

    it('emits PEM material as librdkafka root properties when CA/cert provided', function () {
      // Confluent KafkaJS-compat requires ssl=true inside the kafkaJS block
      // and TLS material as ssl.ca.pem / ssl.certificate.pem / ssl.key.pem at root
      const a = new ConfluentAdapter({
        brokers: ['b:9092'], ssl: 'verify',
        sslCa: 'CA', sslCert: 'CERT', sslKey: 'KEY',
        sslRejectUnauthorized: false
      });
      const cfg = a._buildConfig();
      cfg.kafkaJS.ssl.should.equal(true);
      cfg['ssl.ca.pem'].should.equal('CA');
      cfg['ssl.certificate.pem'].should.equal('CERT');
      cfg['ssl.key.pem'].should.equal('KEY');
      cfg['enable.ssl.certificate.verification'].should.equal(false);
      cfg['security.protocol'].should.equal('SSL');
    });

    it('emits SASL credentials as librdkafka root properties', function () {
      const a = new ConfluentAdapter({
        brokers: ['b:9092'], ssl: true,
        sasl: { mechanism: 'scram-sha-256', username: 'u', password: 'p' }
      });
      const cfg = a._buildConfig();
      cfg['sasl.mechanism'].should.equal('SCRAM-SHA-256');
      cfg['sasl.username'].should.equal('u');
      cfg['sasl.password'].should.equal('p');
      cfg['security.protocol'].should.equal('SASL_SSL');
    });

    it('uses SASL_PLAINTEXT when SASL without SSL', function () {
      const a = new ConfluentAdapter({
        brokers: ['b:9092'],
        sasl: { mechanism: 'plain', username: 'u', password: 'p' }
      });
      const cfg = a._buildConfig();
      cfg['security.protocol'].should.equal('SASL_PLAINTEXT');
      cfg['sasl.mechanism'].should.equal('PLAIN');
    });
  });

  describe('connect() reachability check', function () {
    it('uses a short-lived admin to verify the cluster on connect', async function () {
      const calls = { adminCreated: 0, connected: 0, described: 0, disconnected: 0 };
      const fakeAdmin = {
        connect: async function () { calls.connected++; },
        // The Confluent KafkaJS-compat admin does NOT expose describeCluster();
        // we use listTopics() as the cheap reachability probe instead.
        listTopics: async function () { calls.described++; return []; },
        disconnect: async function () { calls.disconnected++; }
      };
      const a = new ConfluentAdapter({ brokers: ['x:9092'] });
      // Replace the internal Kafka constructor by stubbing _buildConfig and ConfluentKafka.Kafka
      // via prototype shenanigans is too fragile — instead we monkey-patch the class.
      const ConfluentKafkaModule = require('@confluentinc/kafka-javascript').KafkaJS;
      const origKafka = ConfluentKafkaModule.Kafka;
      ConfluentKafkaModule.Kafka = function () {
        return { admin: function () { calls.adminCreated++; return fakeAdmin; } };
      };
      try {
        await a.connect();
        calls.adminCreated.should.equal(1);
        calls.connected.should.equal(1);
        calls.described.should.equal(1);
        calls.disconnected.should.equal(1);
        a.isConnected().should.equal(true);
      } finally {
        ConfluentKafkaModule.Kafka = origKafka;
      }
    });

    it('throws and resets state when the reachability probe fails', async function () {
      const fakeAdmin = {
        connect: async function () {},
        listTopics: async function () { throw new Error('unreachable'); },
        disconnect: async function () {}
      };
      const a = new ConfluentAdapter({ brokers: ['x:9092'] });
      const ConfluentKafkaModule = require('@confluentinc/kafka-javascript').KafkaJS;
      const origKafka = ConfluentKafkaModule.Kafka;
      ConfluentKafkaModule.Kafka = function () {
        return { admin: function () { return fakeAdmin; } };
      };
      try {
        await a.connect()
          .then(() => { throw new Error('should have thrown'); })
          .catch(err => { err.message.should.equal('unreachable'); });
        should(a.kafka).be.null();
        a.isConnected().should.equal(false);
      } finally {
        ConfluentKafkaModule.Kafka = origKafka;
      }
    });
  });

  describe('consumer lifecycle guards', function () {
    it('pause/resume/seek throw before run()', async function () {
      const a = new ConfluentAdapter({ brokers: ['x'] });
      a.kafka = { consumer: () => ({}) };
      const c = a.createConsumer('g');
      (() => c.pause(['t'])).should.throw(/not running/);
      (() => c.resume(['t'])).should.throw(/not running/);
      await c.seek({ topic: 't', partition: 0, offset: 1 })
        .then(() => { throw new Error('should have thrown'); })
        .catch(err => { err.message.should.match(/not running/); });
    });
  });

  describe('producer adapter (mocked kafka)', function () {
    let adapter, producer, capturedProducerOpts, mockProducer, sentMessages;

    beforeEach(function () {
      sentMessages = [];
      mockProducer = {
        connect: async function () { this._connected = true; },
        disconnect: async function () { this._connected = false; },
        send: async function (args) { sentMessages.push(args); return [{ partition: 0, baseOffset: '42' }]; },
        sendBatch: async function (args) { sentMessages.push(args); return []; }
      };
      const mockKafka = {
        producer: function (opts) { capturedProducerOpts = opts; return mockProducer; }
      };
      adapter = new ConfluentAdapter({ brokers: ['x'] });
      adapter.kafka = mockKafka;
      producer = adapter.createProducer({ allowAutoTopicCreation: true });
    });

    it('defers producer creation until first send', async function () {
      await producer.connect();
      should(producer.producer).be.null();
      await producer.send({ topic: 't', messages: [{ key: 'k', value: 'v' }], acks: 1, timeout: 5000, compression: 'gzip' });
      should.exist(producer.producer);
    });

    it('passes acks/timeout/compression as kafkaJS options at producer creation', async function () {
      await producer.connect();
      await producer.send({ topic: 't', messages: [{ value: 'v' }], acks: 1, timeout: 5000, compression: 'gzip' });
      capturedProducerOpts.kafkaJS.acks.should.equal(1);
      capturedProducerOpts.kafkaJS.timeout.should.equal(5000);
      capturedProducerOpts.kafkaJS.compression.should.equal('gzip');
      capturedProducerOpts.kafkaJS.allowAutoTopicCreation.should.equal(true);
    });

    it('does NOT pass compression option when value is "none"', async function () {
      await producer.connect();
      await producer.send({ topic: 't', messages: [{ value: 'v' }], compression: 'none' });
      should(capturedProducerOpts.kafkaJS.compression).be.undefined();
    });

    it('serializes object values to JSON', async function () {
      await producer.connect();
      await producer.send({ topic: 't', messages: [{ value: { a: 1 } }] });
      sentMessages[0].messages[0].value.should.equal('{"a":1}');
    });

    it('passes buffers through unchanged', async function () {
      await producer.connect();
      const buf = Buffer.from('hi');
      await producer.send({ topic: 't', messages: [{ value: buf }] });
      sentMessages[0].messages[0].value.should.equal(buf);
    });

    it('disconnect() is idempotent', async function () {
      await producer.disconnect();
      await producer.disconnect();
    });
  });

  describe('consumer adapter (mocked kafka)', function () {
    let adapter, consumer, capturedConsumerOpts, mockConsumer, runArgs, subscribed;

    beforeEach(function () {
      subscribed = [];
      mockConsumer = {
        connect: async function () {},
        disconnect: async function () {},
        subscribe: async function (s) { subscribed.push(s); },
        run: async function (args) { runArgs = args; },
        commitOffsets: async function () {},
        pause: function () {}, resume: function () {},
        seek: function () {}
      };
      const mockKafka = {
        consumer: function (opts) { capturedConsumerOpts = opts; return mockConsumer; }
      };
      adapter = new ConfluentAdapter({ brokers: ['x'] });
      adapter.kafka = mockKafka;
      consumer = adapter.createConsumer('group-1', { sessionTimeout: 30000 });
    });

    it('defers consumer creation until run()', async function () {
      await consumer.connect();
      await consumer.subscribe({ topics: ['t1'], fromBeginning: true });
      should(consumer.consumer).be.null();
      await consumer.run({ eachMessage: () => {}, autoCommit: false, autoCommitInterval: 1000 });
      should.exist(consumer.consumer);
    });

    it('passes groupId/fromBeginning/autoCommit as kafkaJS options at creation', async function () {
      await consumer.connect();
      await consumer.subscribe({ topics: ['t1'], fromBeginning: true });
      await consumer.run({ eachMessage: () => {}, autoCommit: false, autoCommitInterval: 1000 });
      capturedConsumerOpts.kafkaJS.groupId.should.equal('group-1');
      capturedConsumerOpts.kafkaJS.fromBeginning.should.equal(true);
      capturedConsumerOpts.kafkaJS.autoCommit.should.equal(false);
      capturedConsumerOpts.kafkaJS.autoCommitInterval.should.equal(1000);
      capturedConsumerOpts.kafkaJS.sessionTimeout.should.equal(30000);
    });

    it('subscribes to all pending topics on run()', async function () {
      await consumer.connect();
      await consumer.subscribe({ topics: ['t1', 't2', 't3'] });
      await consumer.run({ eachMessage: () => {} });
      subscribed.length.should.equal(3);
      subscribed.map(s => s.topic).should.deepEqual(['t1', 't2', 't3']);
    });
  });
});
