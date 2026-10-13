'use strict';

/**
 * Parse an integer config value. Unlike `parseInt(v) || fallback`, a valid
 * 0 is kept; only missing/non-numeric values (or values below `min`) fall
 * back to the default.
 */
function intOr(value, fallback, min = -Infinity) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n >= min ? n : fallback;
}

// Optional PROTOCOL:// prefix (PLAINTEXT://, SASL_SSL://, ...), then host:port.
// IPv6 literals are not accepted because kafkajs splits seed brokers on ':'.
const BROKER_RE = /^(?:[A-Za-z_]+:\/\/)?([A-Za-z0-9.-]+:\d{1,5})$/;

/**
 * Normalize a broker address to the bare host:port form both client
 * libraries expect. Returns null when the address is not valid.
 */
function normalizeBroker(address) {
  const m = BROKER_RE.exec(String(address).trim());
  return m ? m[1] : null;
}

/**
 * Exponential backoff: initial, 2x, 4x, ... capped at max.
 */
class Backoff {
  constructor(initial, max) {
    this.initial = Math.max(1, initial);
    this.max = Math.max(this.initial, max);
    this.attempt = 0;
  }

  next() {
    const delay = Math.min(this.max, this.initial * Math.pow(2, this.attempt));
    this.attempt++;
    return delay;
  }

  reset() {
    this.attempt = 0;
  }
}

/**
 * Coerce an arbitrary object into valid Kafka headers. Kafka header values
 * must be strings or Buffers (or arrays of them); numbers/booleans are
 * stringified, objects JSON-encoded, null/undefined entries dropped.
 */
function toKafkaHeaders(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return undefined;
  const coerce = (v) => {
    if (Buffer.isBuffer(v) || typeof v === 'string') return v;
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
  };
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v == null) continue;
    out[k] = Array.isArray(v) ? v.filter(x => x != null).map(coerce) : coerce(v);
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Human-readable retry delay for status/log text: "500ms", "4s".
 */
function formatDelay(ms) {
  return ms < 1000 ? ms + 'ms' : Math.round(ms / 1000) + 's';
}

module.exports = { intOr, normalizeBroker, Backoff, toKafkaHeaders, formatDelay };
