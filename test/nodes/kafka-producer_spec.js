'use strict';

const should = require('should');
const helper = require('node-red-node-test-helper');
const producerNode = require('../../nodes/kafka-suite-producer.js');
const brokerNode = require('../../nodes/kafka-suite-broker.js');

helper.init(require.resolve('node-red'));

describe('kafka-suite-producer node', function () {
  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) { helper.unload().then(() => helper.stopServer(done)); });

  function loadFlow(extraProducerProps, cb) {
    const flow = [
      {
        id: 'b1', type: 'kafka-suite-broker', name: 'b',
        brokers: 'localhost:9092', clientId: 'test',
        backend: 'kafkajs', authType: 'none'
      },
      Object.assign({
        id: 'p1', type: 'kafka-suite-producer', name: 'prod',
        broker: 'b1', topic: 'test-topic', wires: [['h1']]
      }, extraProducerProps || {}),
      { id: 'h1', type: 'helper' }
    ];
    helper.load([brokerNode, producerNode], flow, function () {
      const p = helper.getNode('p1');
      const b = helper.getNode('b1');
      // Stub doConnect to avoid network
      b.doConnect = async function () { b.connected = true; };
      cb(p, b, helper.getNode('h1'));
    });
  }

  it('loads with topic and wires', function (done) {
    loadFlow({}, (p) => {
      should.exist(p);
      p.topic.should.equal('test-topic');
      done();
    });
  });

  it('reports error when no broker is configured', function (done) {
    const flow = [{ id: 'p1', type: 'kafka-suite-producer', topic: 't' }];
    helper.load(producerNode, flow, function () {
      const p = helper.getNode('p1');
      should.exist(p);
      // No brokerNode => producer is not setup
      should(p.producer).be.null();
      done();
    });
  });

  it('returns error when input arrives before producer is connected', function (done) {
    loadFlow({}, (p) => {
      p.receive({ payload: 'x' });
      // node.error is invoked; we wait a tick
      setTimeout(() => {
        // producer is null => error path
        should(p.producer).be.null();
        done();
      }, 50);
    });
  });

  it('returns error when no topic configured and msg.topic missing', function (done) {
    loadFlow({ topic: '' }, (p) => {
      // Inject fake producer
      let sent = null, errored = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      p.error = function (err) { errored = err; };
      p.receive({ payload: 'x' });
      setTimeout(() => {
        should.exist(errored);
        String(errored).should.match(/topic/i);
        done();
      }, 50);
    });
  });

  it('sends single message and emits result with kafka metadata', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '99' }]; }
      };
      h.on('input', function (msg) {
        should.exist(msg.kafka);
        msg.kafka.topic.should.equal('test-topic');
        msg.kafka.offset.should.equal('99');
        msg.kafka.partition.should.equal(0);
        sent.topic.should.equal('test-topic');
        sent.messages.length.should.equal(1);
        sent.messages[0].value.should.equal('hello');
        done();
      });
      p.receive({ payload: 'hello' });
    });
  });

  it('uses msg.topic when no node-config topic', function (done) {
    loadFlow({ topic: '' }, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        sent.topic.should.equal('runtime-topic');
        done();
      });
      p.receive({ payload: 'x', topic: 'runtime-topic' });
    });
  });

  it('passes acks/timeout/compression from node config to producer.send()', function (done) {
    loadFlow({ acks: '1', timeout: '5000', compression: 'gzip' }, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        sent.acks.should.equal(1);
        sent.timeout.should.equal(5000);
        sent.compression.should.equal('gzip');
        done();
      });
      p.receive({ payload: 'x' });
    });
  });

  it('batch mode: sends array payload as multiple messages', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        sent.messages.length.should.equal(3);
        sent.messages.map(m => m.value).should.deepEqual(['a', 'b', 'c']);
        done();
      });
      p.receive({ payload: ['a', 'b', 'c'] });
    });
  });

  it('batch mode: per-item key/partition overrides node-level defaults', function (done) {
    loadFlow({ key: 'default-key' }, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        sent.messages.length.should.equal(2);
        // Item 1 has key + value + partition → unwrapped
        sent.messages[0].key.should.equal('item-key-1');
        sent.messages[0].value.should.equal('v1');
        sent.messages[0].partition.should.equal(2);
        // Item 2 has key + value (no partition/headers) → also unwrapped, key set
        sent.messages[1].key.should.equal('only-key');
        sent.messages[1].value.should.equal('v2');
        done();
      });
      p.receive({
        payload: [
          { key: 'item-key-1', value: 'v1', partition: 2 },
          { key: 'only-key', value: 'v2' }
        ]
      });
    });
  });

  it('passes msg.headers through to producer.send()', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        sent.messages[0].headers.should.deepEqual({ 'x-trace-id': 'abc' });
        done();
      });
      p.receive({ payload: 'x', headers: { 'x-trace-id': 'abc' } });
    });
  });

  it('uses Schema Registry encode() when registry node is set', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null, encodedCalledWith = null;
      p.registryNode = {
        encode: async function (subject, payload) {
          encodedCalledWith = { subject, payload };
          return Buffer.from([0, 0, 0, 0, 1, 65]);
        }
      };
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        encodedCalledWith.subject.should.equal('test-topic-value');
        encodedCalledWith.payload.should.deepEqual({ a: 1 });
        Buffer.isBuffer(sent.messages[0].value).should.equal(true);
        done();
      });
      p.receive({ payload: { a: 1 } });
    });
  });

  it('does NOT treat a record with a "value" field as a wrapped envelope', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null;
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        sent.messages.length.should.equal(2);
        // The Sensor-Event records have a "value" property of their own
        // — that must NOT be unwrapped, the entire object stays the message body.
        sent.messages[0].value.should.deepEqual({ sensor: 'a', value: 1.1 });
        sent.messages[1].value.should.deepEqual({ sensor: 'b', value: 2.2 });
        done();
      });
      p.receive({ payload: [{ sensor: 'a', value: 1.1 }, { sensor: 'b', value: 2.2 }] });
    });
  });

  it('Schema Registry encodes EACH array element separately in batch mode', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null;
      const encodedCalls = [];
      p.registryNode = {
        encode: async function (subject, payload) {
          encodedCalls.push({ subject, payload });
          // Return a unique buffer per call so we can match them
          return Buffer.from([0, 0, 0, 0, 1, encodedCalls.length]);
        }
      };
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        // Three array elements => three independent encode() calls
        encodedCalls.length.should.equal(3);
        encodedCalls[0].subject.should.equal('test-topic-value');
        encodedCalls[0].payload.should.deepEqual({ a: 1 });
        encodedCalls[1].payload.should.deepEqual({ a: 2 });
        encodedCalls[2].payload.should.deepEqual({ a: 3 });
        // Each message in the batch should carry its own encoded buffer
        sent.messages.length.should.equal(3);
        Buffer.isBuffer(sent.messages[0].value).should.equal(true);
        sent.messages[0].value[5].should.equal(1);
        sent.messages[1].value[5].should.equal(2);
        sent.messages[2].value[5].should.equal(3);
        done();
      });
      p.receive({ payload: [{ a: 1 }, { a: 2 }, { a: 3 }] });
    });
  });

  it('Schema Registry encodes per-item value in wrapped batch mode', function (done) {
    loadFlow({}, (p, b, h) => {
      let sent = null;
      const encodedCalls = [];
      p.registryNode = {
        encode: async function (subject, payload) {
          encodedCalls.push({ subject, payload });
          return Buffer.from([0, 0, 0, 0, 1]);
        }
      };
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        encodedCalls.length.should.equal(2);
        encodedCalls[0].payload.should.deepEqual({ x: 'a' });
        encodedCalls[1].payload.should.deepEqual({ x: 'b' });
        sent.messages[0].key.should.equal('k-A');
        sent.messages[0].partition.should.equal(0);
        sent.messages[1].key.should.equal('k-B');
        sent.messages[1].partition.should.equal(2);
        done();
      });
      p.receive({
        payload: [
          { key: 'k-A', value: { x: 'a' }, partition: 0 },
          { key: 'k-B', value: { x: 'b' }, partition: 2 }
        ]
      });
    });
  });

  it('exposes full RecordMetadata array on msg.kafka.results for multi-partition sends', function (done) {
    loadFlow({}, (p, b, h) => {
      const fullResult = [
        { partition: 0, baseOffset: '10' },
        { partition: 1, baseOffset: '7' },
        { partition: 2, baseOffset: '99' }
      ];
      p.producer = { send: async function () { return fullResult; } };
      h.on('input', function (msg) {
        msg.kafka.results.should.deepEqual(fullResult);
        msg.kafka.partition.should.equal(0);
        msg.kafka.offset.should.equal('10');
        done();
      });
      p.receive({ payload: ['a', 'b', 'c'] });
    });
  });

  it('msg.kafka.results is empty array when send returns nothing', function (done) {
    loadFlow({}, (p, b, h) => {
      p.producer = { send: async function () { return undefined; } };
      h.on('input', function (msg) {
        msg.kafka.results.should.deepEqual([]);
        done();
      });
      p.receive({ payload: 'x' });
    });
  });

  it('autoRegister=true + msg.schemaDefinition triggers registerSchema() before encode()', function (done) {
    loadFlow({}, (p, b, h) => {
      const calls = [];
      let sent = null;
      p.registryNode = {
        autoRegister: true,
        registerSchema: async function (subject, schema, type) {
          calls.push({ op: 'register', subject, schema, type });
          return 42;
        },
        encode: async function (subject, payload) {
          calls.push({ op: 'encode', subject, payload });
          return Buffer.from([0, 0, 0, 0, 42, 1]);
        }
      };
      p.producer = {
        send: async function (args) { sent = args; return [{ partition: 0, baseOffset: '1' }]; }
      };
      h.on('input', function () {
        // Order matters: register MUST come before encode for the first send.
        calls[0].op.should.equal('register');
        calls[0].subject.should.equal('test-topic-value');
        calls[0].schema.should.deepEqual({ type: 'record', name: 'X', fields: [] });
        calls[1].op.should.equal('encode');
        Buffer.isBuffer(sent.messages[0].value).should.equal(true);
        done();
      });
      p.receive({
        payload: { a: 1 },
        schemaDefinition: { type: 'record', name: 'X', fields: [] }
      });
    });
  });

  it('autoRegister=false leaves registerSchema untouched even with schemaDefinition', function (done) {
    loadFlow({}, (p, b, h) => {
      let registered = false;
      p.registryNode = {
        autoRegister: false,
        registerSchema: async function () { registered = true; return 1; },
        encode: async function () { return Buffer.from([0, 0, 0, 0, 1, 1]); }
      };
      p.producer = { send: async function () { return [{ partition: 0, baseOffset: '0' }]; } };
      h.on('input', function () {
        registered.should.equal(false);
        done();
      });
      p.receive({
        payload: { a: 1 },
        schemaDefinition: { type: 'record', name: 'X', fields: [] }
      });
    });
  });

  it('autoRegister failure surfaces as node.error and does NOT send', function (done) {
    loadFlow({}, (p) => {
      let sent = false, errored = null;
      p.registryNode = {
        autoRegister: true,
        registerSchema: async function () { throw new Error('SR-403'); },
        encode: async function () { return Buffer.from([0]); }
      };
      p.producer = { send: async function () { sent = true; return []; } };
      p.error = function (err) { errored = err; };
      p.receive({
        payload: { a: 1 },
        schemaDefinition: { type: 'record', name: 'X', fields: [] }
      });
      setTimeout(() => {
        sent.should.equal(false);
        should.exist(errored);
        String(errored.message || errored).should.match(/SR-403/);
        done();
      }, 50);
    });
  });

  it('forwards send() errors via done callback', function (done) {
    loadFlow({}, (p) => {
      p.producer = {
        send: async function () { throw new Error('broker down'); }
      };
      let errored = null;
      p.error = function (err) { errored = err; };
      p.receive({ payload: 'x' });
      setTimeout(() => {
        should.exist(errored);
        String(errored.message || errored).should.match(/broker down/);
        done();
      }, 50);
    });
  });
});
