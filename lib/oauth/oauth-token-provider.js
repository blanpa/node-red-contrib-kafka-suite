'use strict';

/**
 * OAuth 2.0 token provider for SASL/OAUTHBEARER.
 *
 * Mirrors the Strimzi `io.strimzi.kafka.oauth` client login callback handler:
 * it obtains a bearer token from an OAuth 2.0 token endpoint using either the
 * Resource Owner Password Credentials grant ("password") or the Client
 * Credentials grant ("client_credentials"), then hands the raw access token to
 * the Kafka client for SASL/OAUTHBEARER authentication.
 *
 * Strimzi connect.properties equivalents:
 *   oauth.token.endpoint.uri        -> tokenEndpoint
 *   oauth.grant.type                -> grantType
 *   oauth.client.id                 -> clientId
 *   oauth.client.secret             -> clientSecret   (optional)
 *   oauth.password.grant.username   -> username       (password grant)
 *   oauth.password.grant.password   -> password       (password grant)
 *   oauth.scope                     -> scope          (optional)
 *   oauth.audience                  -> audience       (optional)
 *
 * No third-party HTTP dependency: uses the Node built-in http/https modules so
 * the package keeps its near-zero-dependency footprint.
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');

// Refresh the token this many milliseconds before it actually expires, so a
// request in flight never races the expiry. Also used as a floor for very
// short-lived tokens.
const EXPIRY_SKEW_MS = 30 * 1000;
// Fallback lifetime when the token endpoint omits `expires_in`.
const DEFAULT_LIFETIME_MS = 60 * 60 * 1000;

const SUPPORTED_GRANTS = ['password', 'client_credentials'];

function normalizeConfig(oauth) {
  if (!oauth || typeof oauth !== 'object') {
    throw new Error('OAUTHBEARER: missing OAuth configuration');
  }
  const grantType = oauth.grantType || 'client_credentials';
  if (!SUPPORTED_GRANTS.includes(grantType)) {
    throw new Error(
      `OAUTHBEARER: unsupported grant type '${grantType}'. ` +
      `Supported: ${SUPPORTED_GRANTS.join(', ')}.`
    );
  }
  if (!oauth.tokenEndpoint) {
    throw new Error('OAUTHBEARER: token endpoint URL is required');
  }
  if (!oauth.clientId) {
    throw new Error('OAUTHBEARER: client ID is required');
  }
  if (grantType === 'password' && (!oauth.username || !oauth.password)) {
    throw new Error('OAUTHBEARER: username and password are required for the password grant');
  }
  return { ...oauth, grantType };
}

function buildRequestBody(oauth) {
  const params = new URLSearchParams();
  params.set('grant_type', oauth.grantType);
  params.set('client_id', oauth.clientId);
  if (oauth.clientSecret) params.set('client_secret', oauth.clientSecret);
  if (oauth.grantType === 'password') {
    params.set('username', oauth.username);
    params.set('password', oauth.password);
  }
  if (oauth.scope) params.set('scope', oauth.scope);
  if (oauth.audience) params.set('audience', oauth.audience);
  return params.toString();
}

/**
 * Perform a single token request against the configured endpoint.
 * @returns {Promise<{accessToken: string, lifetimeMs: number}>}
 */
function requestToken(oauth) {
  return new Promise((resolve, reject) => {
    let url;
    try {
      url = new URL(oauth.tokenEndpoint);
    } catch (e) {
      reject(new Error(`OAUTHBEARER: invalid token endpoint URL '${oauth.tokenEndpoint}'`));
      return;
    }

    const isHttps = url.protocol === 'https:';
    const transport = isHttps ? https : http;
    const body = buildRequestBody(oauth);

    const options = {
      method: 'POST',
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname + url.search,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    // Only meaningful for https; ignored otherwise. Defaults to verifying.
    if (isHttps && oauth.rejectUnauthorized === false) {
      options.rejectUnauthorized = false;
    }

    const req = transport.request(options, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(
            `OAUTHBEARER: token request failed (HTTP ${res.statusCode}): ` +
            truncate(text, 500)
          ));
          return;
        }
        let json;
        try {
          json = JSON.parse(text);
        } catch (e) {
          reject(new Error('OAUTHBEARER: token endpoint returned non-JSON response'));
          return;
        }
        const accessToken = json.access_token;
        if (!accessToken) {
          reject(new Error('OAUTHBEARER: token response did not contain an access_token'));
          return;
        }
        const expiresIn = Number(json.expires_in);
        const lifetimeMs = Number.isFinite(expiresIn) && expiresIn > 0
          ? expiresIn * 1000
          : DEFAULT_LIFETIME_MS;
        resolve({ accessToken, lifetimeMs });
      });
    });

    req.on('error', (err) => {
      reject(new Error(`OAUTHBEARER: token request error: ${err.message}`));
    });
    req.write(body);
    req.end();
  });
}

function truncate(str, max) {
  if (typeof str !== 'string') return '';
  return str.length > max ? str.slice(0, max) + '…' : str;
}

/**
 * Create a caching token fetcher. Returns an async function that resolves to a
 * fresh access token string, reusing a cached token until it nears expiry.
 *
 * @param {object} oauthConfig
 * @param {object} [deps] - injectable for testing (deps.requestToken, deps.now)
 * @returns {() => Promise<string>}
 */
function createTokenFetcher(oauthConfig, deps = {}) {
  const oauth = normalizeConfig(oauthConfig);
  const doRequest = deps.requestToken || requestToken;
  const now = deps.now || (() => Date.now());

  let cachedToken = null;
  let expiresAt = 0;
  let pending = null;

  return async function getToken() {
    if (cachedToken && now() < expiresAt) {
      return cachedToken;
    }
    // Collapse concurrent refreshes into a single in-flight request.
    if (pending) return pending;

    pending = (async () => {
      const { accessToken, lifetimeMs } = await doRequest(oauth);
      cachedToken = accessToken;
      // Refresh slightly early; never set a negative/zero window.
      const window = Math.max(lifetimeMs - EXPIRY_SKEW_MS, Math.min(lifetimeMs, EXPIRY_SKEW_MS));
      expiresAt = now() + window;
      return accessToken;
    })();

    try {
      return await pending;
    } finally {
      pending = null;
    }
  };
}

/**
 * Create a KafkaJS-compatible `oauthBearerProvider`.
 * KafkaJS expects an async function returning `{ value: <token> }`.
 *
 * @param {object} oauthConfig
 * @param {object} [deps]
 * @returns {() => Promise<{value: string}>}
 */
function createOAuthBearerProvider(oauthConfig, deps = {}) {
  const getToken = createTokenFetcher(oauthConfig, deps);
  return async function oauthBearerProvider() {
    const value = await getToken();
    return { value };
  };
}

module.exports = {
  createOAuthBearerProvider,
  createTokenFetcher,
  requestToken,
  normalizeConfig,
  buildRequestBody,
  SUPPORTED_GRANTS,
  EXPIRY_SKEW_MS,
  DEFAULT_LIFETIME_MS
};
