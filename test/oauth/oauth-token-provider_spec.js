'use strict';

const should = require('should');
const {
  createOAuthBearerProvider,
  createTokenFetcher,
  normalizeConfig,
  buildRequestBody,
  EXPIRY_SKEW_MS
} = require('../../lib/oauth/oauth-token-provider');

const PW_CONFIG = {
  tokenEndpoint: 'https://idp.example.com/oauth/token',
  grantType: 'password',
  clientId: 'kafka-client',
  username: 'alice',
  password: 's3cret'
};

const CC_CONFIG = {
  tokenEndpoint: 'https://idp.example.com/oauth/token',
  grantType: 'client_credentials',
  clientId: 'kafka-client',
  clientSecret: 'topsecret'
};

describe('oauth-token-provider', function () {
  describe('normalizeConfig', function () {
    it('defaults grantType to client_credentials', function () {
      const c = normalizeConfig({ tokenEndpoint: 'https://x/t', clientId: 'c' });
      c.grantType.should.equal('client_credentials');
    });

    it('rejects unsupported grant type', function () {
      (() => normalizeConfig({ ...CC_CONFIG, grantType: 'implicit' }))
        .should.throw(/unsupported grant type/);
    });

    it('requires token endpoint', function () {
      (() => normalizeConfig({ clientId: 'c' })).should.throw(/token endpoint/);
    });

    it('requires client id', function () {
      (() => normalizeConfig({ tokenEndpoint: 'https://x/t' })).should.throw(/client ID/);
    });

    it('requires username/password for password grant', function () {
      (() => normalizeConfig({ ...PW_CONFIG, password: '' }))
        .should.throw(/username and password/);
    });
  });

  describe('buildRequestBody', function () {
    it('encodes password grant fields', function () {
      const body = buildRequestBody(normalizeConfig(PW_CONFIG));
      const params = new URLSearchParams(body);
      params.get('grant_type').should.equal('password');
      params.get('client_id').should.equal('kafka-client');
      params.get('username').should.equal('alice');
      params.get('password').should.equal('s3cret');
    });

    it('encodes client_credentials with secret, scope, audience', function () {
      const body = buildRequestBody(normalizeConfig({
        ...CC_CONFIG, scope: 'kafka', audience: 'https://kafka'
      }));
      const params = new URLSearchParams(body);
      params.get('grant_type').should.equal('client_credentials');
      params.get('client_secret').should.equal('topsecret');
      params.get('scope').should.equal('kafka');
      params.get('audience').should.equal('https://kafka');
      should(params.get('username')).be.null();
    });
  });

  describe('createOAuthBearerProvider', function () {
    it('returns { value: token } from the token endpoint', async function () {
      let calls = 0;
      const provider = createOAuthBearerProvider(PW_CONFIG, {
        requestToken: async () => {
          calls++;
          return { accessToken: 'tok-1', lifetimeMs: 3600 * 1000 };
        }
      });
      const result = await provider();
      result.should.deepEqual({ value: 'tok-1' });
      calls.should.equal(1);
    });
  });

  describe('createTokenFetcher caching', function () {
    it('reuses the cached token until near expiry', async function () {
      let now = 1000;
      let calls = 0;
      const getToken = createTokenFetcher(CC_CONFIG, {
        now: () => now,
        requestToken: async () => {
          calls++;
          return { accessToken: 'tok-' + calls, lifetimeMs: 3600 * 1000 };
        }
      });

      (await getToken()).should.equal('tok-1');
      now += 1000; // still well within lifetime
      (await getToken()).should.equal('tok-1');
      calls.should.equal(1);
    });

    it('refreshes once the token nears expiry', async function () {
      let now = 0;
      let calls = 0;
      const lifetimeMs = 100 * 1000;
      const getToken = createTokenFetcher(CC_CONFIG, {
        now: () => now,
        requestToken: async () => {
          calls++;
          return { accessToken: 'tok-' + calls, lifetimeMs };
        }
      });

      (await getToken()).should.equal('tok-1');
      // Advance past (lifetime - skew) so the cache is considered stale.
      now += lifetimeMs - EXPIRY_SKEW_MS + 1;
      (await getToken()).should.equal('tok-2');
      calls.should.equal(2);
    });

    it('collapses concurrent refreshes into one request', async function () {
      let calls = 0;
      const getToken = createTokenFetcher(CC_CONFIG, {
        now: () => 0,
        requestToken: async () => {
          calls++;
          await new Promise((r) => setTimeout(r, 10));
          return { accessToken: 'tok', lifetimeMs: 3600 * 1000 };
        }
      });

      const [a, b] = await Promise.all([getToken(), getToken()]);
      a.should.equal('tok');
      b.should.equal('tok');
      calls.should.equal(1);
    });

    it('propagates token endpoint errors', async function () {
      const getToken = createTokenFetcher(CC_CONFIG, {
        now: () => 0,
        requestToken: async () => { throw new Error('HTTP 401: invalid_client'); }
      });
      await getToken().should.be.rejectedWith(/invalid_client/);
    });
  });
});
