'use strict';

const should = require('should');
const sinon = require('sinon');
const { CompressionTypes, CompressionCodecs } = require('kafkajs');

describe('kafkajs-codecs', function () {
  let codecs;

  before(function () {
    codecs = require('../../lib/kafkajs-codecs');
  });

  it('always reports built-in codecs as available', function () {
    codecs.isAvailable('none').should.be.true();
    codecs.isAvailable('gzip').should.be.true();
  });

  it('exposes a status object covering snappy/lz4/zstd', function () {
    codecs.status.should.have.property('snappy');
    codecs.status.should.have.property('lz4');
    codecs.status.should.have.property('zstd');
  });

  it('packageFor returns the package name for known codec names', function () {
    codecs.packageFor('lz4').should.equal('kafkajs-lz4');
    codecs.packageFor('zstd').should.equal('@kafkajs/zstd');
    codecs.packageFor('snappy').should.equal('kafkajs-snappy');
  });

  it('registers CompressionCodecs entries for codecs whose packages resolved', function () {
    for (const name of ['snappy', 'lz4', 'zstd']) {
      if (codecs.status[name].available) {
        const type = CompressionTypes[name === 'snappy' ? 'Snappy'
          : name === 'lz4' ? 'LZ4' : 'ZSTD'];
        should.exist(CompressionCodecs[type]);
      }
    }
  });
});

describe('kafkajs-adapter compression resolution', function () {
  // The adapter's compression validator is exercised through the producer
  // send() path; we stub the underlying kafkajs producer so we can assert
  // on the error surfaced when a codec is unavailable, without needing a
  // live broker.
  const KafkaJSAdapter = require('../../lib/adapters/kafkajs-adapter');
  const codecs = require('../../lib/kafkajs-codecs');
  const { Kafka } = require('kafkajs');

  function buildProducerAdapter() {
    const adapter = new KafkaJSAdapter({ brokers: ['localhost:9092'] });
    adapter.kafka = new Kafka({ brokers: ['localhost:9092'], clientId: 't' });
    const producer = adapter.createProducer();
    sinon.stub(producer.producer, 'send').resolves([]);
    sinon.stub(producer.producer, 'sendBatch').resolves([]);
    return producer;
  }

  it('accepts compression="none" without requiring any codec', async function () {
    const p = buildProducerAdapter();
    await p.send({ topic: 't', messages: [{ value: 'x' }], compression: 'none' });
    p.producer.send.calledOnce.should.be.true();
  });

  it('accepts compression=undefined without requiring any codec', async function () {
    const p = buildProducerAdapter();
    await p.send({ topic: 't', messages: [{ value: 'x' }] });
    p.producer.send.calledOnce.should.be.true();
  });

  it('throws a clear error when an unavailable codec is selected', async function () {
    const p = buildProducerAdapter();
    // Force "lz4" to look unavailable for this test even if kafkajs-lz4 is installed
    const orig = codecs.isAvailable;
    codecs.isAvailable = (name) => (name === 'lz4' ? false : orig(name));
    try {
      let err;
      try {
        await p.send({ topic: 't', messages: [{ value: 'x' }], compression: 'lz4' });
      } catch (e) { err = e; }
      should.exist(err);
      err.message.should.match(/LZ4 compression codec is not registered/);
      err.message.should.match(/kafkajs-lz4/);
    } finally {
      codecs.isAvailable = orig;
    }
  });

  it('rejects unknown compression names', async function () {
    const p = buildProducerAdapter();
    let err;
    try {
      await p.send({ topic: 't', messages: [{ value: 'x' }], compression: 'bogus' });
    } catch (e) { err = e; }
    should.exist(err);
    err.message.should.match(/Unknown compression type: bogus/);
  });
});
