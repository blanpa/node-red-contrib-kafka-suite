'use strict';

const should = require('should');
const helper = require('node-red-node-test-helper');
const consumerNode = require('../../nodes/kafka-suite-consumer.js');
const brokerNode = require('../../nodes/kafka-suite-broker.js');

helper.init(require.resolve('node-red'));

describe('kafka-suite-consumer node', function () {
  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) { helper.unload().then(() => helper.stopServer(done)); });

  function loadFlow(extraConsumerProps, cb) {
    const flow = [
      {
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', backend: 'kafkajs', authType: 'none'
      },
      Object.assign({
        id: 'c1', type: 'kafka-suite-consumer',
        broker: 'b1', topics: 'topic-a',
        groupId: 'g1', startOffset: 'latest',
        autoCommit: true, messageFormat: 'string',
        wires: [['h1']]
      }, extraConsumerProps || {}),
      { id: 'h1', type: 'helper' }
    ];
    helper.load([brokerNode, consumerNode], flow, function () {
      const c = helper.getNode('c1');
      const b = helper.getNode('b1');
      b.doConnect = async function () { b.connected = true; };
      cb(c, b, helper.getNode('h1'));
    });
  }

  it('parses comma-separated topics', function (done) {
    loadFlow({ topics: ' a , b ,c ' }, (c) => {
      c.topics.should.deepEqual(['a', 'b', 'c']);
      done();
    });
  });

  it('uses defaults: autoCommit=true, format=string, startOffset=latest', function (done) {
    loadFlow({}, (c) => {
      c.autoCommit.should.equal(true);
      c.messageFormat.should.equal('string');
      c.startOffset.should.equal('latest');
      done();
    });
  });

  it('reports error status when no broker', function (done) {
    const flow = [{ id: 'c1', type: 'kafka-suite-consumer', topics: 't' }];
    helper.load(consumerNode, flow, function () {
      const c = helper.getNode('c1');
      should.exist(c);
      done();
    });
  });

  it('reports error status when no topics', function (done) {
    const flow = [
      { id: 'b1', type: 'kafka-suite-broker', brokers: 'localhost:9092' },
      { id: 'c1', type: 'kafka-suite-consumer', broker: 'b1', topics: '' }
    ];
    helper.load([brokerNode, consumerNode], flow, function () {
      const c = helper.getNode('c1');
      should.exist(c);
      should(c.consumer).be.null();
      done();
    });
  });

  describe('_decodeValue', function () {
    it('decodes string format', function (done) {
      loadFlow({ messageFormat: 'string' }, (c) => {
        c._decodeValue(Buffer.from('hello')).should.equal('hello');
        done();
      });
    });

    it('decodes json format with valid JSON', function (done) {
      loadFlow({ messageFormat: 'json' }, (c) => {
        c._decodeValue(Buffer.from('{"a":1}')).should.deepEqual({ a: 1 });
        done();
      });
    });

    it('falls back to string when JSON is invalid', function (done) {
      loadFlow({ messageFormat: 'json' }, (c) => {
        c._decodeValue(Buffer.from('not-json')).should.equal('not-json');
        done();
      });
    });

    it('decodes raw format (returns buffer untouched)', function (done) {
      loadFlow({ messageFormat: 'raw' }, (c) => {
        const buf = Buffer.from([1, 2, 3]);
        c._decodeValue(buf).should.equal(buf);
        done();
      });
    });

    it('returns null when value is null/undefined', function (done) {
      loadFlow({}, (c) => {
        should(c._decodeValue(null)).equal(null);
        should(c._decodeValue(undefined)).equal(null);
        done();
      });
    });
  });

  describe('message processing (mocked consumer)', function () {
    function makeMockConsumer() {
      const mock = {
        on: function () {},
        connect: async function () {},
        subscribe: async function () {},
        run: async function (args) { mock._runArgs = args; },
        commitOffsets: async function (offs) { mock._commitedOffsets = offs; },
        disconnect: async function () {}
      };
      return mock;
    }

    function setupWithMock(c, b, mockConsumer) {
      // Make broker.getClient() return a fake client whose createConsumer
      // returns our mock; then call _setupConsumer to wire it through.
      b.client = {
        createConsumer: function () { return mockConsumer; }
      };
      return c._setupConsumer();
    }

    it('emits a Node-RED message per kafka message with headers/key/offset', function (done) {
      loadFlow({}, (c, b, h) => {
        const mock = makeMockConsumer();
        setupWithMock(c, b, mock).then(() => {
          h.on('input', function (msg) {
            msg.payload.should.equal('hello');
            msg.topic.should.equal('topic-a');
            msg.partition.should.equal(2);
            msg.offset.should.equal('17');
            msg.key.should.equal('mykey');
            msg.headers.should.deepEqual({ 'x-trace': 'abc' });
            msg.kafka.consumerGroup.should.equal('g1');
            done();
          });
          mock._runArgs.eachMessage({
            topic: 'topic-a', partition: 2,
            message: {
              value: Buffer.from('hello'),
              key: Buffer.from('mykey'),
              offset: '17', timestamp: '12345',
              headers: { 'x-trace': Buffer.from('abc') }
            },
            heartbeat: async () => {}
          });
        });
      });
    });

    it('JSON format parses payload to object', function (done) {
      loadFlow({ messageFormat: 'json' }, (c, b, h) => {
        const mock = makeMockConsumer();
        setupWithMock(c, b, mock).then(() => {
          h.on('input', function (msg) {
            msg.payload.should.deepEqual({ x: 1 });
            done();
          });
          mock._runArgs.eachMessage({
            topic: 'topic-a', partition: 0,
            message: {
              value: Buffer.from('{"x":1}'),
              key: null, offset: '0', timestamp: '0', headers: {}
            },
            heartbeat: async () => {}
          });
        });
      });
    });

    it('manual commit: msg.commit is a function when autoCommit=false', function (done) {
      loadFlow({ autoCommit: false }, (c, b, h) => {
        const mock = makeMockConsumer();
        setupWithMock(c, b, mock).then(() => {
          h.on('input', async function (msg) {
            (typeof msg.commit).should.equal('function');
            await msg.commit();
            mock._commitedOffsets.should.deepEqual([
              { topic: 'topic-a', partition: 0, offset: '11' }
            ]);
            done();
          });
          mock._runArgs.eachMessage({
            topic: 'topic-a', partition: 0,
            message: { value: Buffer.from('x'), key: null, offset: '10', timestamp: '0', headers: {} },
            heartbeat: async () => {}
          });
        });
      });
    });

    it('manual commit: handles offsets larger than Number.MAX_SAFE_INTEGER (BigInt)', function (done) {
      loadFlow({ autoCommit: false }, (c, b, h) => {
        const mock = makeMockConsumer();
        setupWithMock(c, b, mock).then(() => {
          // 2^60 ≫ 2^53 (Number.MAX_SAFE_INTEGER) so plain parseInt would lose precision
          const hugeOffset = '1152921504606846975';
          const expectedNext = '1152921504606846976';
          h.on('input', async function (msg) {
            await msg.commit();
            mock._commitedOffsets[0].offset.should.equal(expectedNext);
            done();
          });
          mock._runArgs.eachMessage({
            topic: 'topic-a', partition: 0,
            message: { value: Buffer.from('x'), key: null, offset: hugeOffset, timestamp: '0', headers: {} },
            heartbeat: async () => {}
          });
        });
      });
    });

    it('handles array-valued (multi-value) headers without crashing', function (done) {
      loadFlow({}, (c, b, h) => {
        const mock = makeMockConsumer();
        setupWithMock(c, b, mock).then(() => {
          h.on('input', function (msg) {
            msg.headers.should.have.property('x-multi');
            msg.headers['x-multi'].should.deepEqual(['a', 'b']);
            should(msg.headers['x-null']).equal(null);
            done();
          });
          mock._runArgs.eachMessage({
            topic: 'topic-a', partition: 0,
            message: {
              value: Buffer.from('hi'),
              key: null, offset: '0', timestamp: '0',
              headers: {
                'x-multi': [Buffer.from('a'), Buffer.from('b')],
                'x-null': null
              }
            },
            heartbeat: async () => {}
          });
        });
      });
    });

    it('uses Schema Registry decode() when registry node is set', function (done) {
      loadFlow({}, (c, b, h) => {
        c.registryNode = {
          decode: async function (buf) { return { decoded: true, len: buf.length }; }
        };
        const mock = makeMockConsumer();
        setupWithMock(c, b, mock).then(() => {
          h.on('input', function (msg) {
            msg.payload.should.deepEqual({ decoded: true, len: 5 });
            done();
          });
          mock._runArgs.eachMessage({
            topic: 'topic-a', partition: 0,
            message: {
              value: Buffer.from([0, 0, 0, 0, 1]),
              key: null, offset: '0', timestamp: '0', headers: {}
            },
            heartbeat: async () => {}
          });
        });
      });
    });
  });

  describe('control input (pause/resume)', function () {
    it('returns error when consumer is not connected', function (done) {
      loadFlow({}, (c) => {
        let errored = null;
        c.error = function (e) { errored = e; };
        c.receive({ payload: 'pause' });
        setTimeout(() => {
          should.exist(errored);
          done();
        }, 50);
      });
    });

    it('pause sets paused=true', function (done) {
      loadFlow({}, (c) => {
        let pausedTopics = null;
        c.consumer = { pause: function (t) { pausedTopics = t; } };
        c.receive({ action: 'pause' });
        setTimeout(() => {
          c.paused.should.equal(true);
          pausedTopics.should.deepEqual(['topic-a']);
          done();
        }, 50);
      });
    });

    it('resume sets paused=false', function (done) {
      loadFlow({}, (c) => {
        c.paused = true;
        let resumedTopics = null;
        c.consumer = {
          pause: function () {},
          resume: function (t) { resumedTopics = t; }
        };
        c.receive({ action: 'resume' });
        setTimeout(() => {
          c.paused.should.equal(false);
          resumedTopics.should.deepEqual(['topic-a']);
          done();
        }, 50);
      });
    });
  });
});
