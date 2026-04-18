'use strict';

const should = require('should');
const helper = require('node-red-node-test-helper');
const brokerNode = require('../../nodes/kafka-suite-broker.js');

helper.init(require.resolve('node-red'));

describe('kafka-suite-broker node', function () {
  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) {
    helper.unload().then(() => helper.stopServer(done));
  });

  it('is loaded with defaults', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker', name: 'b',
      brokers: 'localhost:9092', clientId: 'test',
      backend: 'kafkajs', authType: 'none'
    }];
    helper.load(brokerNode, flow, function () {
      const n = helper.getNode('b1');
      should.exist(n);
      n.brokers.should.deepEqual(['localhost:9092']);
      n.clientId.should.equal('test');
      n.backend.should.equal('kafkajs');
      n.authType.should.equal('none');
      done();
    });
  });

  it('parses multiple comma-separated brokers', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker',
      brokers: 'a:9092, b:9092 ,c:9092'
    }];
    helper.load(brokerNode, flow, function () {
      const n = helper.getNode('b1');
      n.brokers.should.deepEqual(['a:9092', 'b:9092', 'c:9092']);
      done();
    });
  });

  it('buildAdapterConfig: none auth has no ssl/sasl', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker',
      brokers: 'localhost:9092', authType: 'none'
    }];
    helper.load(brokerNode, flow, function () {
      const n = helper.getNode('b1');
      const cfg = n.getAdapterConfig();
      should(cfg.ssl).be.undefined();
      should(cfg.sasl).be.undefined();
      done();
    });
  });

  it('buildAdapterConfig: sasl-plain triggers ssl + sasl', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker',
      brokers: 'b:9092', authType: 'sasl-plain'
    }];
    const creds = { b1: { username: 'u', password: 'p' } };
    helper.load(brokerNode, flow, creds, function () {
      const n = helper.getNode('b1');
      const cfg = n.getAdapterConfig();
      cfg.ssl.should.equal(true);
      cfg.sasl.mechanism.should.equal('plain');
      cfg.sasl.username.should.equal('u');
      cfg.sasl.password.should.equal('p');
      done();
    });
  });

  it('buildAdapterConfig: sasl-scram-512 mechanism mapping', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker',
      brokers: 'b:9092', authType: 'sasl-scram-512'
    }];
    const creds = { b1: { username: 'u', password: 'p' } };
    helper.load(brokerNode, flow, creds, function () {
      const n = helper.getNode('b1');
      const cfg = n.getAdapterConfig();
      cfg.sasl.mechanism.should.equal('scram-sha-512');
      done();
    });
  });

  it('buildAdapterConfig: ssl auth passes CA/cert/key', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker',
      brokers: 'b:9092', authType: 'ssl'
    }];
    const creds = { b1: { sslCa: 'CA', sslCert: 'CE', sslKey: 'KE' } };
    helper.load(brokerNode, flow, creds, function () {
      const n = helper.getNode('b1');
      const cfg = n.getAdapterConfig();
      cfg.ssl.should.equal(true);
      cfg.sslCa.should.equal('CA');
      cfg.sslCert.should.equal('CE');
      cfg.sslKey.should.equal('KE');
      should(cfg.sasl).be.undefined();
      done();
    });
  });

  it('register/deregister tracks users', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker', brokers: 'localhost:9092'
    }];
    helper.load(brokerNode, flow, function () {
      const n = helper.getNode('b1');
      const fake = { id: 'child1', status: () => {} };
      n.doConnect = async () => { n.connected = true; };
      n.register(fake);
      Object.keys(n.users).length.should.equal(1);
      n.deregister(fake, () => {
        Object.keys(n.users).length.should.equal(0);
        done();
      }, false);
    });
  });

  describe('broker URL validation', function () {
    it('flags invalid broker URLs', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092, no-port-here, also bad,bad:port'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        n.invalidBrokers.length.should.equal(3);
        n.invalidBrokers.should.containEql('no-port-here');
        n.invalidBrokers.should.containEql('also bad');
        n.invalidBrokers.should.containEql('bad:port');
        done();
      });
    });

    it('accepts host:port and PROTOCOL://host:port forms', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'kafka1.example.com:9092,SASL_SSL://broker:9094,127.0.0.1:9092'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        n.invalidBrokers.should.deepEqual([]);
        done();
      });
    });

    it('doConnect aborts and reports error for invalid brokers', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'invalid-no-port'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        let errored = null;
        n.error = function (e) { errored = e; };
        n.doConnect().then(() => {
          should.exist(errored);
          String(errored).should.match(/Invalid broker URL/);
          n.connected.should.equal(false);
          should(n.client).be.null();
          done();
        });
      });
    });
  });

  describe('reconnect on lost connection', function () {
    it('register-during-connecting sets yellow status on the new child', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker', brokers: 'localhost:9092'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        // Pretend a first child already kicked off doConnect() and we are
        // currently mid-flight (connecting=true, connected=false).
        n.users['first'] = { id: 'first', status: () => {} };
        n.connecting = true;
        let childStatus = null;
        const lateChild = { id: 'late', status: function (s) { childStatus = s; } };
        n.register(lateChild);
        should.exist(childStatus);
        childStatus.fill.should.equal('yellow');
        childStatus.text.should.match(/connecting/);
        done();
      });
    });

    it('_handleClientLost schedules reconnect and resets state', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', initialRetryTime: '50'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        // Simulate a successful connection
        n.connected = true;
        n.client = { disconnect: async () => {}, on: () => {} };
        // Keep at least one user so the reconnect will fire
        n.users['fake'] = { id: 'fake', status: () => {} };
        let reconnectAttempted = false;
        n.doConnect = async function () { reconnectAttempted = true; };
        n._handleClientLost('test');
        n.connected.should.equal(false);
        should(n.client).be.null();
        setTimeout(() => {
          reconnectAttempted.should.equal(true);
          done();
        }, 120);
      });
    });

    it('does not reconnect if no users remain', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', initialRetryTime: '30'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        n.connected = true;
        n.client = { disconnect: async () => {}, on: () => {} };
        // No users registered
        let attempts = 0;
        n.doConnect = async function () { attempts++; };
        n._handleClientLost('test');
        setTimeout(() => {
          attempts.should.equal(0);
          done();
        }, 80);
      });
    });
  });
});
