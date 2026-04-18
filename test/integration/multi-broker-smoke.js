#!/usr/bin/env node
/* eslint-disable no-console */
'use strict';

/**
 * Multi-broker E2E smoke test:
 * runs the SAME flow (admin → producer → consumer → schema-registry round-trip)
 * against the user-configured BROKER endpoints and BACKENDS.
 *
 * Why: we have one source-of-truth code path; the only thing that changes
 * between Confluent CP, Redpanda and Aiven is the broker config. Smoke-testing
 * each combination guards against subtle adapter divergence.
 *
 *   BROKERS_CP=localhost:9092
 *   BROKERS_REDPANDA=localhost:9192
 *   SCHEMA_REGISTRY=http://localhost:8081
 *   BACKENDS=kafkajs,confluent       (default: kafkajs)
 *
 * Aiven cannot be tested without credentials — for Aiven we only run a
 * config-validation pass (URL + cert format checks).
 */

const path = require('path');
const fs = require('fs');
const should = require('should');
const { Kafka } = require('kafkajs');

let ConfluentKafka = null;
try { ConfluentKafka = require('@confluentinc/kafka-javascript').KafkaJS; } catch (e) { /* optional */ }

let SchemaRegistry = null;
try { SchemaRegistry = require('@kafkajs/confluent-schema-registry').SchemaRegistry; } catch (e) { /* optional */ }

const ClientFactory = require(path.join('..', '..', 'lib', 'client-factory.js'));

const certsDir = path.join(__dirname, '..', 'certs');
function readCert(name) {
  const p = path.join(certsDir, name);
  return fs.existsSync(p) ? fs.readFileSync(p, 'utf8') : null;
}

const targets = [];

// Plain PLAINTEXT clusters
if (process.env.BROKERS_CP) targets.push({ name: 'Confluent CP (cp-kafka)', brokers: process.env.BROKERS_CP });
if (process.env.BROKERS_REDPANDA) targets.push({ name: 'Redpanda', brokers: process.env.BROKERS_REDPANDA });

// Aiven-sim: mTLS-only (CA + client cert + client key required)
if (process.env.BROKERS_AIVEN_SIM) {
  const ca = readCert('ca.pem');
  const cert = readCert('client.crt');
  const key = readCert('client.key.pem');
  if (ca && cert && key) {
    targets.push({
      name: 'Aiven-sim (mTLS)',
      brokers: process.env.BROKERS_AIVEN_SIM,
      ssl: { ca: [ca], cert: cert, key: key, rejectUnauthorized: false }
    });
  } else {
    console.error('[Aiven-sim] WARNING: certs missing in', certsDir, '— run scripts/gen-test-certs.sh');
  }
}

// Redpanda with SASL_SSL (SCRAM-SHA-256)
if (process.env.BROKERS_REDPANDA_SASL) {
  const ca = readCert('ca.pem');
  if (ca) {
    targets.push({
      name: 'Redpanda-SASL (SCRAM-SHA-256 + SSL)',
      brokers: process.env.BROKERS_REDPANDA_SASL,
      ssl: { ca: [ca], rejectUnauthorized: false },
      sasl: {
        mechanism: 'scram-sha-256',
        username: process.env.SASL_USER || 'app',
        password: process.env.SASL_PASS || 'app-secret'
      }
    });
  }
}

if (!targets.length) {
  targets.push({ name: 'Confluent CP (default)', brokers: 'localhost:9092' });
  targets.push({ name: 'Redpanda (default)', brokers: 'localhost:9192' });
}

const backends = (process.env.BACKENDS || 'kafkajs').split(',').map(s => s.trim()).filter(Boolean);
const SR_URL = process.env.SCHEMA_REGISTRY || 'http://localhost:8081';

function log(scope, msg) { console.log('[' + scope + '] ' + msg); }

async function smoke(target, backend) {
  const scope = target.name + ' / ' + backend;
  log(scope, 'starting');

  const config = {
    brokers: target.brokers.split(',').map(s => s.trim()),
    clientId: 'smoke-' + backend + '-' + Date.now(),
    backend,
    connectionTimeout: 5000,
    requestTimeout: 10000
  };
  if (target.ssl) {
    config.ssl = target.ssl;
    if (target.ssl.ca) {
      config.sslCa = Array.isArray(target.ssl.ca) ? target.ssl.ca[0] : target.ssl.ca;
    }
    if (target.ssl.cert) config.sslCert = target.ssl.cert;
    if (target.ssl.key) config.sslKey = target.ssl.key;
    config.sslRejectUnauthorized = target.ssl.rejectUnauthorized !== false;
  }
  if (target.sasl) {
    config.sasl = target.sasl;
  }

  const client = ClientFactory.createClient(backend, config);
  await client.connect();

  // ---- ADMIN ----
  const admin = client.createAdmin();
  await admin.connect();
  const topic = 'smoke-' + backend + '-' + Date.now();
  await admin.createTopics([{ topic, numPartitions: 3, replicationFactor: 1 }]);
  log(scope, 'created topic ' + topic);
  const allTopics = await admin.listTopics();
  allTopics.includes(topic).should.equal(true, 'topic missing in listTopics');

  // ---- PRODUCER (single + batch) ----
  const producer = client.createProducer();
  await producer.connect();
  const single = await producer.send({
    topic,
    messages: [{ key: 'k1', value: Buffer.from('hello-' + backend) }],
    acks: -1
  });
  single[0].partition.should.be.a.Number();
  log(scope, 'single send OK at partition ' + single[0].partition);

  const batch = await producer.send({
    topic,
    messages: [
      { key: 'a', value: Buffer.from('msg-1') },
      { key: 'b', value: Buffer.from('msg-2') },
      { key: 'c', value: Buffer.from('msg-3') }
    ],
    acks: -1
  });
  batch.length.should.be.greaterThan(0);
  log(scope, 'batch send OK across ' + batch.length + ' partitions');

  // ---- CONSUMER ----
  const groupId = 'smoke-grp-' + Date.now();
  const consumer = client.createConsumer(groupId, {});
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  const seen = [];
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('consume timeout, only got ' + seen.length)), 15000);
    consumer.run({
      autoCommit: true,
      eachMessage: async ({ message, partition }) => {
        seen.push({
          key: message.key && message.key.toString(),
          value: message.value && message.value.toString(),
          partition
        });
        if (seen.length >= 4) { clearTimeout(timer); resolve(); }
      }
    }).catch(reject);
  });
  await consumer.disconnect();
  seen.length.should.be.greaterThanOrEqual(4);
  log(scope, 'consumed ' + seen.length + ' messages OK');

  // ---- SCHEMA REGISTRY round-trip (only if SR available + cp-kafka) ----
  if (SchemaRegistry && SR_URL && /cp/i.test(target.name)) {
    const registry = new SchemaRegistry({ host: SR_URL });
    const subject = topic + '-value';
    const schema = JSON.stringify({
      type: 'record', name: 'SmokeEvent', namespace: 'smoke',
      fields: [{ name: 'id', type: 'int' }, { name: 'msg', type: 'string' }]
    });
    const reg = await registry.register({ type: 'AVRO', schema }, { subject });
    log(scope, 'registered schema id=' + reg.id);

    const enc = await registry.encode(reg.id, { id: 1, msg: 'avro-' + backend });
    const r = await producer.send({
      topic,
      messages: [{ key: 'avro', value: enc }],
      acks: -1
    });
    r[0].partition.should.be.a.Number();
    log(scope, 'avro produce OK');

    // consume just one avro message to round-trip
    const cons2 = client.createConsumer(groupId + '-avro', {});
    await cons2.connect();
    await cons2.subscribe({ topics: [topic], fromBeginning: true });
    let decoded = null;
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('avro consume timeout')), 15000);
      cons2.run({
        autoCommit: true,
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          try {
            const obj = await registry.decode(message.value);
            if (obj && obj.msg && String(obj.msg).startsWith('avro')) {
              decoded = obj; clearTimeout(t); resolve();
            }
          } catch (e) { /* skip non-avro records */ }
        }
      }).catch(reject);
    });
    await cons2.disconnect();
    should.exist(decoded);
    decoded.id.should.be.a.Number();
    log(scope, 'avro round-trip OK: ' + JSON.stringify(decoded));
  } else {
    log(scope, 'skipping Schema Registry round-trip (SR not available or non-CP target)');
  }

  // ---- TEARDOWN ----
  await producer.disconnect();
  await admin.deleteTopics([topic]);
  await admin.disconnect();
  log(scope, 'PASS');
}

function aivenConfigValidation() {
  log('Aiven', 'config-only validation (no live cluster)');
  // Aiven uses mTLS — verify our broker code path produces the right adapter config.
  const cfg = {
    brokers: ['kafka-foo.aivencloud.com:23456'],
    clientId: 'aiven-smoke',
    backend: 'kafkajs',
    ssl: {
      rejectUnauthorized: true,
      ca: '-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----',
      cert: '-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----',
      key: '-----BEGIN PRIVATE KEY-----\nFAKEKEY\n-----END PRIVATE KEY-----'
    }
  };
  // We don't connect — just instantiate to make sure the adapter accepts the shape.
  const client = ClientFactory.createClient('kafkajs', cfg);
  should.exist(client.createProducer);
  should.exist(client.createConsumer);
  should.exist(client.createAdmin);
  log('Aiven', 'PASS (adapter accepts mTLS config shape)');
}

(async function main() {
  try {
    aivenConfigValidation();
    for (const t of targets) {
      for (const b of backends) {
        if (b === 'confluent' && !ConfluentKafka) {
          log(t.name + ' / confluent', 'SKIP (@confluentinc/kafka-javascript not installed)');
          continue;
        }
        await smoke(t, b);
      }
    }
    console.log('\nALL E2E SMOKE TESTS PASSED');
    process.exit(0);
  } catch (err) {
    console.error('\nSMOKE FAILED:', err);
    process.exit(1);
  }
})();
