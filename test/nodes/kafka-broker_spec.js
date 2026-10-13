'use strict';

const should = require('should');
const helper = require('node-red-node-test-helper');
const sinon = require('sinon');
const brokerNode = require('../../nodes/kafka-suite-broker.js');
const KafkaJSAdapter = require('../../lib/adapters/kafkajs-adapter');

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

  it('buildAdapterConfig: sasl-oauthbearer builds oauth config from settings + creds', function (done) {
    const flow = [{
      id: 'b1', type: 'kafka-suite-broker',
      brokers: 'b:9092', authType: 'sasl-oauthbearer',
      oauthTokenEndpoint: 'https://idp.example.com/oauth/token',
      oauthGrantType: 'password',
      oauthScope: 'kafka', oauthAudience: 'https://kafka',
      oauthTlsReject: false
    }];
    const creds = { b1: {
      oauthClientId: 'cid', oauthClientSecret: 'sec',
      oauthUsername: 'alice', oauthPassword: 's3cret'
    } };
    helper.load(brokerNode, flow, creds, function () {
      const n = helper.getNode('b1');
      const cfg = n.getAdapterConfig();
      cfg.ssl.should.equal(true);
      cfg.sasl.mechanism.should.equal('oauthbearer');
      const o = cfg.sasl.oauth;
      o.tokenEndpoint.should.equal('https://idp.example.com/oauth/token');
      o.grantType.should.equal('password');
      o.clientId.should.equal('cid');
      o.clientSecret.should.equal('sec');
      o.username.should.equal('alice');
      o.password.should.equal('s3cret');
      o.scope.should.equal('kafka');
      o.audience.should.equal('https://kafka');
      o.rejectUnauthorized.should.equal(false);
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
        // The prefix is stripped: kafkajs splits seed brokers on ':'.
        n.brokers.should.deepEqual(['kafka1.example.com:9092', 'broker:9094', '127.0.0.1:9092']);
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

    it('register on an already connected broker starts the child right away', function (done) {
      const flow = [{ id: 'b1', type: 'kafka-suite-broker', brokers: 'localhost:9092' }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        n.connected = true;
        let started = false;
        n.register({ id: 'c', status: () => {}, _onBrokerConnected: () => { started = true; } });
        started.should.equal(true);
        done();
      });
    });

    it('_scheduleReconnect backs off exponentially up to maxRetryTime', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', initialRetryTime: '20', maxRetryTime: '70'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        n.doConnect = async function () {};
        const delays = [n._scheduleReconnect(), n._scheduleReconnect(), n._scheduleReconnect(), n._scheduleReconnect()];
        delays.should.deepEqual([20, 40, 70, 70]);
        done();
      });
    });

    it('reconnect timer calls doConnect while users remain', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', initialRetryTime: '30'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        n.users['fake'] = { id: 'fake', status: () => {} };
        let attempts = 0;
        n.doConnect = async function () { attempts++; };
        n._scheduleReconnect();
        setTimeout(() => {
          attempts.should.equal(1);
          done();
        }, 80);
      });
    });

    it('does not reconnect if no users remain', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'localhost:9092', initialRetryTime: '30'
      }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        let attempts = 0;
        n.doConnect = async function () { attempts++; };
        n._scheduleReconnect();
        setTimeout(() => {
          attempts.should.equal(0);
          done();
        }, 80);
      });
    });
  });

  describe('doConnect (stubbed adapter)', function () {
    let connectStub;
    beforeEach(function () { connectStub = sinon.stub(KafkaJSAdapter.prototype, 'connect'); });
    afterEach(function () { connectStub.restore(); });

    function load(cb) {
      const flow = [{ id: 'b1', type: 'kafka-suite-broker', brokers: 'localhost:9092', initialRetryTime: '1000' }];
      helper.load(brokerNode, flow, function () {
        const n = helper.getNode('b1');
        // The flow load did not register anything, so no connect ran yet.
        cb(n);
      });
    }

    it('notifies registered children once connected', function (done) {
      connectStub.resolves();
      load((n) => {
        let started = 0;
        n.users['c'] = { id: 'c', status: () => {}, _onBrokerConnected: () => { started++; } };
        n.doConnect().then(() => {
          n.connected.should.equal(true);
          should.exist(n.client);
          started.should.equal(1);
          done();
        });
      });
    });

    it('schedules a reconnect with backoff after a failed connect', function (done) {
      connectStub.rejects(new Error('ECONNREFUSED'));
      load((n) => {
        n.users['c'] = { id: 'c', status: () => {} };
        let logged = null;
        n.error = function (e) { logged = e; };
        n.doConnect().then(() => {
          n.connected.should.equal(false);
          should(n.client).be.null();
          should.exist(n._reconnectTimer);
          String(logged).should.match(/ECONNREFUSED.*retrying in 1s/);
          done();
        });
      });
    });

    it('drops a connect that finishes after doDisconnect()', function (done) {
      let release;
      connectStub.callsFake(() => new Promise(resolve => { release = resolve; }));
      load((n) => {
        const pending = n.doConnect();
        n.doDisconnect().then(() => {
          release();
          return pending;
        }).then(() => {
          n.connected.should.equal(false);
          should(n.client).be.null();
          done();
        });
      });
    });
  });

  describe('SASL TLS toggle', function () {
    it('saslSsl=false yields SASL_PLAINTEXT (sasl without ssl)', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'b:9092', authType: 'sasl-scram-256', saslSsl: false
      }];
      const creds = { b1: { username: 'u', password: 'p' } };
      helper.load(brokerNode, flow, creds, function () {
        const cfg = helper.getNode('b1').getAdapterConfig();
        should(cfg.ssl).be.undefined();
        cfg.sasl.mechanism.should.equal('scram-sha-256');
        done();
      });
    });

    it('missing saslSsl (older configs) keeps TLS on and passes a custom CA', function (done) {
      const flow = [{
        id: 'b1', type: 'kafka-suite-broker',
        brokers: 'b:9092', authType: 'sasl-plain'
      }];
      const creds = { b1: { username: 'u', password: 'p', sslCa: 'CA' } };
      helper.load(brokerNode, flow, creds, function () {
        const cfg = helper.getNode('b1').getAdapterConfig();
        cfg.ssl.should.equal(true);
        cfg.sslCa.should.equal('CA');
        done();
      });
    });
  });

  it('keeps retries=0 instead of replacing it with the default', function (done) {
    const flow = [{ id: 'b1', type: 'kafka-suite-broker', brokers: 'b:9092', retries: '0' }];
    helper.load(brokerNode, flow, function () {
      helper.getNode('b1').getAdapterConfig().retries.should.equal(0);
      done();
    });
  });
});
