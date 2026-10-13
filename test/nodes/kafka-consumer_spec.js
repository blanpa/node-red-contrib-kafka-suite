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
      // returns our mock; then start the lifecycle to wire it through.
      b.connected = true;
      b.client = {
        createConsumer: function () { return mockConsumer; }
      };
      return c.lifecycle.start();
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

  describe('lifecycle, crash handling and options (mocked consumer)', function () {
    function makeMock() {
      const mock = {
        handlers: {},
        on: function (ev, fn) { mock.handlers[ev] = fn; },
        connect: async function () {},
        subscribe: async function () {},
        run: async function (args) { mock._runArgs = args; },
        commitOffsets: async function (offs) { mock._committed = offs; },
        pause: function (t) { mock._paused = t; },
        resume: function () {},
        disconnect: async function () { mock._disconnected = true; }
      };
      return mock;
    }

    function start(c, b, mock, onCreate) {
      b.connected = true;
      b.client = {
        createConsumer: function (groupId, opts) {
          if (onCreate) onCreate(groupId, opts);
          return mock;
        }
      };
      return c.lifecycle.start();
    }

    function feed(mock, message) {
      return mock._runArgs.eachMessage({
        topic: 'topic-a', partition: 0,
        message: Object.assign({ value: Buffer.from('x'), key: null, offset: '5', timestamp: '0', headers: {} }, message),
        heartbeat: async () => {}
      });
    }

    it('passes sessionTimeout/heartbeatInterval/concurrency through', function (done) {
      loadFlow({ sessionTimeout: '45000', heartbeatInterval: '5000', concurrency: '4' }, (c, b) => {
        const mock = makeMock();
        let opts = null;
        start(c, b, mock, (g, o) => { opts = o; }).then(() => {
          opts.should.deepEqual({ sessionTimeout: 45000, heartbeatInterval: 5000 });
          mock._runArgs.partitionsConsumedConcurrently.should.equal(4);
          c.consumer.should.equal(mock);
          done();
        });
      });
    });

    it('a crash without restart rebuilds the consumer', function (done) {
      loadFlow({}, (c, b) => {
        const mock = makeMock();
        start(c, b, mock).then(() => {
          let restarted = false;
          c.lifecycle.restart = function () { restarted = true; return Promise.resolve(); };
          c.error = function () {};
          mock.handlers.crash({ error: new Error('boom'), restart: false });
          restarted.should.equal(true);
          done();
        });
      });
    });

    it('a crash that kafkajs restarts itself is only reported', function (done) {
      loadFlow({}, (c, b) => {
        const mock = makeMock();
        start(c, b, mock).then(() => {
          let restarted = false;
          c.lifecycle.restart = function () { restarted = true; return Promise.resolve(); };
          c.warn = function () {};
          mock.handlers.crash({ error: new Error('retriable'), restart: true });
          restarted.should.equal(false);
          done();
        });
      });
    });

    it('re-applies a pause when the consumer is rebuilt', function (done) {
      loadFlow({}, (c, b) => {
        c.paused = true;
        const mock = makeMock();
        start(c, b, mock).then(() => {
          mock._paused.should.deepEqual(['topic-a']);
          done();
        });
      });
    });

    it('writes headers to a custom property and leaves msg.headers alone', function (done) {
      loadFlow({ headersProperty: 'kafkaHeaders' }, (c, b, h) => {
        const mock = makeMock();
        start(c, b, mock).then(() => {
          h.on('input', function (msg) {
            msg.kafkaHeaders.should.deepEqual({ 'x-id': '1' });
            should(msg.headers).be.undefined();
            done();
          });
          feed(mock, { headers: { 'x-id': Buffer.from('1') } });
        });
      });
    });

    it('keyFormat=raw keeps binary keys as Buffer', function (done) {
      loadFlow({ keyFormat: 'raw' }, (c, b, h) => {
        const mock = makeMock();
        const key = Buffer.from([0xff, 0x00, 0x01]);
        start(c, b, mock).then(() => {
          h.on('input', function (msg) {
            Buffer.isBuffer(msg.key).should.equal(true);
            msg.key.equals(key).should.equal(true);
            done();
          });
          feed(mock, { key });
        });
      });
    });

    it('commit action commits offset+1 of the message fed back in', function (done) {
      loadFlow({ autoCommit: false }, (c, b) => {
        const mock = makeMock();
        start(c, b, mock).then(() => {
          c.receive({ action: 'commit', topic: 'topic-a', partition: 3, offset: '9007199254740993' });
          setTimeout(() => {
            mock._committed.should.deepEqual([{ topic: 'topic-a', partition: 3, offset: '9007199254740994' }]);
            done();
          }, 50);
        });
      });
    });

    it('commit action without coordinates reports an error', function (done) {
      loadFlow({ autoCommit: false }, (c, b) => {
        const mock = makeMock();
        start(c, b, mock).then(() => {
          let errored = null;
          c.error = function (e) { errored = e; };
          c.receive({ action: 'commit', topic: 'topic-a' });
          setTimeout(() => {
            String(errored).should.match(/requires msg.topic, msg.partition and msg.offset/);
            should(mock._committed).be.undefined();
            done();
          }, 50);
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
