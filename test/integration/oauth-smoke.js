#!/usr/bin/env node
/* eslint-disable no-console */
'use strict';

/**
 * OAUTHBEARER end-to-end smoke test.
 *
 * Brought up by docker-compose.oauth.yml:
 *   - Keycloak (OIDC IdP) on http://localhost:8080, realm "kafka"
 *   - Redpanda with native OIDC OAUTHBEARER validation on localhost:9096
 *
 * This exercises the FULL chain that this feature adds:
 *   1. our token provider fetches a real bearer token from Keycloak
 *      (password grant AND client_credentials grant), then
 *   2. our broker adapter authenticates to Redpanda via SASL/OAUTHBEARER and
 *      runs an admin → produce → consume round-trip.
 *
 * Env:
 *   OAUTH_BROKERS    default localhost:9096
 *   OAUTH_TOKEN_URL  default http://localhost:8080/realms/kafka/protocol/openid-connect/token
 *   BACKENDS         default kafkajs  (e.g. "kafkajs,confluent")
 */

const path = require('path');
const should = require('should');
const ClientFactory = require(path.join('..', '..', 'lib', 'client-factory.js'));
const {
  createTokenFetcher
} = require(path.join('..', '..', 'lib', 'oauth', 'oauth-token-provider.js'));

let ConfluentKafka = null;
try { ConfluentKafka = require('@confluentinc/kafka-javascript').KafkaJS; } catch (e) { /* optional */ }

const BROKERS = (process.env.OAUTH_BROKERS || 'localhost:9096').split(',').map(s => s.trim());
const TOKEN_URL = process.env.OAUTH_TOKEN_URL ||
  'http://localhost:8088/realms/kafka/protocol/openid-connect/token';
const BACKENDS = (process.env.BACKENDS || 'kafkajs').split(',').map(s => s.trim()).filter(Boolean);

const GRANTS = {
  password: {
    grantType: 'password',
    tokenEndpoint: TOKEN_URL,
    clientId: 'kafka-client',
    clientSecret: 'kafka-client-secret',
    username: 'alice',
    password: 'alice-password'
  },
  client_credentials: {
    grantType: 'client_credentials',
    tokenEndpoint: TOKEN_URL,
    clientId: 'kafka-client',
    clientSecret: 'kafka-client-secret'
  }
};

function log(scope, msg) { console.log('[' + scope + '] ' + msg); }

// Standalone token sanity check: prove the IdP actually issues a usable token
// for each grant before we involve a broker.
async function checkTokenAcquisition(grantName) {
  const fetch = createTokenFetcher(GRANTS[grantName]);
  const token = await fetch();
  token.should.be.a.String();
  token.split('.').length.should.equal(3, 'expected a JWT (header.payload.signature)');
  const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
  const aud = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  aud.includes('kafka').should.equal(true, 'token audience must include "kafka"; got ' + JSON.stringify(payload.aud));
  log('token:' + grantName, 'OK sub=' + payload.sub + ' aud=' + JSON.stringify(payload.aud));
}

async function smoke(backend, grantName) {
  const scope = backend + ' / ' + grantName;
  log(scope, 'starting OAUTHBEARER round-trip');

  const config = {
    brokers: BROKERS,
    clientId: 'oauth-smoke-' + backend + '-' + Date.now(),
    backend,
    connectionTimeout: 8000,
    requestTimeout: 12000,
    // Redpanda OAUTHBEARER listener here is plaintext (no TLS); SASL still applies.
    ssl: false,
    sasl: {
      mechanism: 'oauthbearer',
      oauth: GRANTS[grantName]
    }
  };

  const client = ClientFactory.createClient(backend, config);
  await client.connect();

  const admin = client.createAdmin();
  await admin.connect();
  const topic = 'oauth-smoke-' + backend + '-' + Date.now();
  await admin.createTopics([{ topic, numPartitions: 1, replicationFactor: 1 }]);
  log(scope, 'created topic ' + topic);

  const producer = client.createProducer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key: 'k', value: Buffer.from('oauth-' + grantName) }],
    acks: -1
  });
  log(scope, 'produced 1 message');

  const consumer = client.createConsumer('oauth-grp-' + Date.now(), {});
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  let got = null;
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('consume timeout')), 15000);
    consumer.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        got = message.value && message.value.toString();
        clearTimeout(t); resolve();
      }
    }).catch(reject);
  });
  await consumer.disconnect();
  got.should.equal('oauth-' + grantName);
  log(scope, 'consumed: ' + got);

  await producer.disconnect();
  await admin.deleteTopics([topic]);
  await admin.disconnect();
  log(scope, 'PASS');
}

(async function main() {
  try {
    log('setup', 'token endpoint ' + TOKEN_URL);
    log('setup', 'brokers ' + BROKERS.join(','));

    await checkTokenAcquisition('password');
    await checkTokenAcquisition('client_credentials');

    for (const backend of BACKENDS) {
      if (backend === 'confluent' && !ConfluentKafka) {
        log(backend, 'SKIP (@confluentinc/kafka-javascript not installed)');
        continue;
      }
      // password grant is kafkajs-only; on confluent only client_credentials.
      const grants = backend === 'confluent' ? ['client_credentials'] : ['password', 'client_credentials'];
      for (const g of grants) {
        await smoke(backend, g);
      }
    }

    console.log('\nALL OAUTHBEARER E2E TESTS PASSED');
    process.exit(0);
  } catch (err) {
    console.error('\nOAUTHBEARER SMOKE FAILED:', err && err.stack ? err.stack : err);
    process.exit(1);
  }
})();
