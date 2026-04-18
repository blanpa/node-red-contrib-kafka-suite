'use strict';

/**
 * Service-preset & config validation tests.
 * Aiven, Confluent Cloud, AWS MSK and Azure Event Hubs cannot be tested
 * against live clusters in CI without secrets — so we verify the adapter
 * config that the broker node *would* hand to the underlying client. If a
 * preset silently produces the wrong shape (no SSL, no SASL mechanism, etc.)
 * we want CI to fail, not the user's first deployment.
 */

const should = require('should');
const helper = require('node-red-node-test-helper');
const brokerNode = require('../nodes/kafka-suite-broker.js');

helper.init(require.resolve('node-red'));

function loadBroker(props, creds, cb) {
  const flow = [Object.assign({
    id: 'b1',
    type: 'kafka-suite-broker',
    name: 'broker-under-test'
  }, props)];
  const credentialsBlock = creds ? { b1: creds } : undefined;
  helper.load(brokerNode, flow, credentialsBlock, function () {
    cb(helper.getNode('b1'));
  });
}

describe('service presets / adapter config shapes', function () {
  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) { helper.unload().then(() => helper.stopServer(done)); });

  describe('Confluent Cloud (SASL/PLAIN over SSL)', function () {
    it('produces SSL + SASL config from username/password', function (done) {
      loadBroker({
        brokers: 'pkc-abcde.eu-central-1.aws.confluent.cloud:9092',
        servicePreset: 'confluent-cloud',
        authType: 'sasl-plain'
      }, { username: 'API_KEY', password: 'API_SECRET' }, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.brokers.should.deepEqual(['pkc-abcde.eu-central-1.aws.confluent.cloud:9092']);
        cfg.ssl.should.equal(true);
        cfg.sslRejectUnauthorized.should.equal(true);
        cfg.sasl.mechanism.should.equal('plain');
        cfg.sasl.username.should.equal('API_KEY');
        cfg.sasl.password.should.equal('API_SECRET');
        done();
      });
    });
  });

  describe('AWS MSK (SCRAM-SHA-512 over SSL)', function () {
    it('maps sasl-scram-512 to scram-sha-512 mechanism', function (done) {
      loadBroker({
        brokers: 'b-1.cluster.kafka.eu-west-1.amazonaws.com:9096',
        servicePreset: 'aws-msk-scram',
        authType: 'sasl-scram-512'
      }, { username: 'msk-user', password: 'msk-pass' }, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.ssl.should.equal(true);
        cfg.sasl.mechanism.should.equal('scram-sha-512');
        cfg.sasl.username.should.equal('msk-user');
        done();
      });
    });
  });

  describe('AWS MSK IAM (OAUTHBEARER)', function () {
    it('SSL on, sasl mechanism oauthbearer, no static credentials', function (done) {
      loadBroker({
        brokers: 'b-1.iamcluster.kafka.us-east-1.amazonaws.com:9098',
        servicePreset: 'aws-msk-iam',
        authType: 'sasl-oauthbearer'
      }, {}, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.ssl.should.equal(true);
        cfg.sasl.mechanism.should.equal('oauthbearer');
        // OAuth must NOT carry static username/password
        should(cfg.sasl.username).be.undefined();
        should(cfg.sasl.password).be.undefined();
        done();
      });
    });
  });

  describe('Azure Event Hubs (SASL/PLAIN with $ConnectionString)', function () {
    it('uses sasl-plain over SSL', function (done) {
      loadBroker({
        brokers: 'my-namespace.servicebus.windows.net:9093',
        servicePreset: 'azure-event-hubs',
        authType: 'sasl-plain'
      }, {
        username: '$ConnectionString',
        password: 'Endpoint=sb://...;SharedAccessKey=...'
      }, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.ssl.should.equal(true);
        cfg.sasl.mechanism.should.equal('plain');
        cfg.sasl.username.should.equal('$ConnectionString');
        done();
      });
    });
  });

  describe('Aiven (mTLS — no live cluster needed)', function () {
    it('passes CA, client cert, client key and passphrase through', function (done) {
      loadBroker({
        brokers: 'kafka-foo-aivenproject.aivencloud.com:21931',
        servicePreset: 'aiven',
        authType: 'ssl',
        sslRejectUnauthorized: true
      }, {
        sslCa: '-----BEGIN CERTIFICATE-----\nFAKE_CA\n-----END CERTIFICATE-----',
        sslCert: '-----BEGIN CERTIFICATE-----\nFAKE_CLIENT\n-----END CERTIFICATE-----',
        sslKey: '-----BEGIN PRIVATE KEY-----\nFAKE_KEY\n-----END PRIVATE KEY-----',
        sslPassphrase: 'optional'
      }, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.brokers[0].should.match(/aivencloud\.com:\d+/);
        cfg.ssl.should.equal(true);
        cfg.sslRejectUnauthorized.should.equal(true);
        cfg.sslCa.should.match(/FAKE_CA/);
        cfg.sslCert.should.match(/FAKE_CLIENT/);
        cfg.sslKey.should.match(/FAKE_KEY/);
        cfg.sslPassphrase.should.equal('optional');
        // Aiven uses mTLS, NOT SASL — make sure we didn't accidentally
        // include a bogus sasl block.
        should(cfg.sasl).be.undefined();
        done();
      });
    });

    it('rejects invalid Aiven broker URLs (must have port)', function (done) {
      loadBroker({
        brokers: 'kafka-foo-aivenproject.aivencloud.com',
        servicePreset: 'aiven',
        authType: 'ssl'
      }, {}, function (b) {
        b.invalidBrokers.length.should.equal(1);
        done();
      });
    });
  });

  describe('Redpanda (Kafka wire-compatible, plaintext or SASL)', function () {
    it('plaintext: no ssl, no sasl', function (done) {
      loadBroker({
        brokers: 'redpanda-1.example.com:9092,redpanda-2.example.com:9092',
        servicePreset: 'redpanda',
        authType: 'none'
      }, {}, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.brokers.length.should.equal(2);
        should(cfg.ssl).be.undefined();
        should(cfg.sasl).be.undefined();
        done();
      });
    });

    it('Redpanda Cloud with SCRAM-SHA-256 produces correct sasl shape', function (done) {
      loadBroker({
        brokers: 'rpck-foo.bytewax.io:9092',
        servicePreset: 'redpanda',
        authType: 'sasl-scram-256'
      }, { username: 'rp-user', password: 'rp-pw' }, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.ssl.should.equal(true);
        cfg.sasl.mechanism.should.equal('scram-sha-256');
        cfg.sasl.username.should.equal('rp-user');
        done();
      });
    });
  });

  describe('self-hosted Kafka (default)', function () {
    it('plain localhost: no ssl, no sasl', function (done) {
      loadBroker({
        brokers: 'localhost:9092',
        servicePreset: 'self-hosted',
        authType: 'none'
      }, {}, function (b) {
        const cfg = b.getAdapterConfig();
        cfg.brokers.should.deepEqual(['localhost:9092']);
        should(cfg.ssl).be.undefined();
        should(cfg.sasl).be.undefined();
        done();
      });
    });
  });

  describe('cross-preset invariants', function () {
    const matrix = [
      { preset: 'confluent-cloud', authType: 'sasl-plain', expectSsl: true, expectSasl: 'plain' },
      { preset: 'aws-msk-scram', authType: 'sasl-scram-512', expectSsl: true, expectSasl: 'scram-sha-512' },
      { preset: 'aws-msk-iam', authType: 'sasl-oauthbearer', expectSsl: true, expectSasl: 'oauthbearer' },
      { preset: 'azure-event-hubs', authType: 'sasl-plain', expectSsl: true, expectSasl: 'plain' },
      { preset: 'aiven', authType: 'ssl', expectSsl: true, expectSasl: null },
      { preset: 'redpanda', authType: 'none', expectSsl: false, expectSasl: null },
      { preset: 'self-hosted', authType: 'none', expectSsl: false, expectSasl: null }
    ];

    matrix.forEach(({ preset, authType, expectSsl, expectSasl }) => {
      it('preset=' + preset + ' / authType=' + authType + ' produces expected ssl=' + expectSsl + ' sasl=' + expectSasl, function (done) {
        loadBroker({
          brokers: 'host.example.com:9092',
          servicePreset: preset,
          authType: authType
        }, { username: 'u', password: 'p' }, function (b) {
          const cfg = b.getAdapterConfig();
          if (expectSsl) cfg.ssl.should.equal(true);
          else should(cfg.ssl).be.undefined();
          if (expectSasl) cfg.sasl.mechanism.should.equal(expectSasl);
          else should(cfg.sasl).be.undefined();
          done();
        });
      });
    });
  });
});
