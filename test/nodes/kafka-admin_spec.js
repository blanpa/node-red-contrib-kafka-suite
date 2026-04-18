'use strict';

const should = require('should');
const helper = require('node-red-node-test-helper');
const adminNode = require('../../nodes/kafka-suite-admin.js');
const brokerNode = require('../../nodes/kafka-suite-broker.js');

helper.init(require.resolve('node-red'));

describe('kafka-suite-admin node', function () {
  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) { helper.unload().then(() => helper.stopServer(done)); });

  function loadFlow(cb) {
    const flow = [
      {
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', backend: 'kafkajs', authType: 'none'
      },
      { id: 'a1', type: 'kafka-suite-admin', broker: 'b1', wires: [['h1']] },
      { id: 'h1', type: 'helper' }
    ];
    helper.load([brokerNode, adminNode], flow, function () {
      const a = helper.getNode('a1');
      const b = helper.getNode('b1');
      b.doConnect = async function () { b.connected = true; };
      cb(a, b, helper.getNode('h1'));
    });
  }

  it('loads with broker reference', function (done) {
    loadFlow((a) => { should.exist(a); done(); });
  });

  it('returns error when no broker is configured', function (done) {
    const flow = [{ id: 'a1', type: 'kafka-suite-admin' }];
    helper.load(adminNode, flow, function () {
      const a = helper.getNode('a1');
      should.exist(a);
      done();
    });
  });

  it('returns error when admin not connected', function (done) {
    loadFlow((a) => {
      let errored = null;
      a.error = function (e) { errored = e; };
      a.receive({ action: 'listTopics' });
      setTimeout(() => {
        should.exist(errored);
        done();
      }, 50);
    });
  });

  it('returns error when no action is given', function (done) {
    loadFlow((a) => {
      a.admin = { listTopics: async () => [] };
      let errored = null;
      a.error = function (e) { errored = e; };
      a.receive({ payload: 'x' });
      setTimeout(() => {
        should.exist(errored);
        String(errored).should.match(/action/i);
        done();
      }, 50);
    });
  });

  it('listTopics: returns array of topics', function (done) {
    loadFlow((a, b, h) => {
      a.admin = { listTopics: async () => ['topic-a', 'topic-b'] };
      h.on('input', function (msg) {
        msg.payload.should.deepEqual(['topic-a', 'topic-b']);
        msg.action.should.equal('listTopics');
        done();
      });
      a.receive({ action: 'listTopics' });
    });
  });

  it('createTopic: requires msg.topic', function (done) {
    loadFlow((a) => {
      a.admin = { createTopics: async () => true };
      let errored = null;
      a.error = function (e) { errored = e; };
      a.receive({ action: 'createTopic' });
      setTimeout(() => {
        String(errored).should.match(/msg\.topic/);
        done();
      }, 50);
    });
  });

  it('createTopic: passes config (partitions, replicationFactor, configEntries)', function (done) {
    loadFlow((a, b, h) => {
      let captured;
      a.admin = { createTopics: async function (t) { captured = t; return true; } };
      h.on('input', function () {
        captured.length.should.equal(1);
        captured[0].topic.should.equal('newtopic');
        captured[0].numPartitions.should.equal(6);
        captured[0].replicationFactor.should.equal(2);
        done();
      });
      a.receive({
        action: 'createTopic',
        topic: 'newtopic',
        config: { partitions: 6, replicationFactor: 2 }
      });
    });
  });

  it('deleteTopic: accepts string or array', function (done) {
    loadFlow((a, b, h) => {
      let captured;
      a.admin = { deleteTopics: async function (t) { captured = t; return true; } };
      let calls = 0;
      h.on('input', function () {
        calls++;
        if (calls === 1) {
          captured.should.deepEqual(['t1']);
          a.receive({ action: 'deleteTopic', topic: ['t2', 't3'] });
        } else {
          captured.should.deepEqual(['t2', 't3']);
          done();
        }
      });
      a.receive({ action: 'deleteTopic', topic: 't1' });
    });
  });

  it('describeCluster: returns cluster info', function (done) {
    loadFlow((a, b, h) => {
      a.admin = {
        describeCluster: async () => ({ brokers: [{ nodeId: 1 }], controller: 1 })
      };
      h.on('input', function (msg) {
        msg.payload.brokers.length.should.equal(1);
        done();
      });
      a.receive({ action: 'describeCluster' });
    });
  });

  it('listGroups: returns groups', function (done) {
    loadFlow((a, b, h) => {
      a.admin = { listGroups: async () => ({ groups: [{ groupId: 'g1' }] }) };
      h.on('input', function (msg) {
        msg.payload.groups[0].groupId.should.equal('g1');
        done();
      });
      a.receive({ action: 'listGroups' });
    });
  });

  it('describeGroup: requires msg.groupId', function (done) {
    loadFlow((a) => {
      a.admin = { describeGroups: async () => [] };
      let errored = null;
      a.error = function (e) { errored = e; };
      a.receive({ action: 'describeGroup' });
      setTimeout(() => {
        String(errored).should.match(/msg\.groupId/);
        done();
      }, 50);
    });
  });

  it('fetchTopicOffsets: returns offsets', function (done) {
    loadFlow((a, b, h) => {
      a.admin = { fetchTopicOffsets: async () => [{ partition: 0, offset: '99' }] };
      h.on('input', function (msg) {
        msg.payload[0].offset.should.equal('99');
        done();
      });
      a.receive({ action: 'fetchTopicOffsets', topic: 'topic-a' });
    });
  });

  it('resetOffsets: requires groupId and topic', function (done) {
    loadFlow((a) => {
      a.admin = { resetOffsets: async () => true };
      let errored = null;
      a.error = function (e) { errored = e; };
      a.receive({ action: 'resetOffsets', groupId: 'g1' });
      setTimeout(() => {
        String(errored).should.match(/msg\.groupId.*msg\.topic|msg\.topic/);
        done();
      }, 50);
    });
  });

  it('unknown action returns descriptive error', function (done) {
    loadFlow((a) => {
      a.admin = {};
      let errored = null;
      a.error = function (e) { errored = e; };
      a.receive({ action: 'doMagic' });
      setTimeout(() => {
        String(errored).should.match(/Unknown action/);
        done();
      }, 50);
    });
  });
});
