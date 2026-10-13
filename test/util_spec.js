'use strict';

const should = require('should');
const { intOr, normalizeBroker, Backoff, toKafkaHeaders, formatDelay } = require('../lib/util');

describe('lib/util', function () {
  describe('intOr', function () {
    it('keeps a valid 0 instead of falling back', function () {
      intOr('0', 5, 0).should.equal(0);
      intOr(0, 5).should.equal(0);
    });

    it('falls back on missing, empty or non-numeric values', function () {
      intOr(undefined, 5).should.equal(5);
      intOr('', 5).should.equal(5);
      intOr('abc', 5).should.equal(5);
    });

    it('falls back when below the minimum', function () {
      intOr('0', 3000, 1).should.equal(3000);
      intOr('-1', 1, 1).should.equal(1);
      intOr('-1', 0).should.equal(-1);
    });
  });

  describe('normalizeBroker', function () {
    it('passes host:port through', function () {
      normalizeBroker('kafka.example.com:9092').should.equal('kafka.example.com:9092');
      normalizeBroker(' 127.0.0.1:9092 ').should.equal('127.0.0.1:9092');
    });

    it('strips a PROTOCOL:// prefix (any case)', function () {
      normalizeBroker('SASL_SSL://broker:9094').should.equal('broker:9094');
      normalizeBroker('plaintext://broker:9092').should.equal('broker:9092');
    });

    it('rejects addresses without a port or with invalid characters', function () {
      should(normalizeBroker('no-port')).be.null();
      should(normalizeBroker('SASL_SSL://no-port')).be.null();
      should(normalizeBroker('bad host:9092')).be.null();
      should(normalizeBroker('host:port')).be.null();
    });
  });

  describe('Backoff', function () {
    it('doubles up to the cap and resets', function () {
      const b = new Backoff(100, 500);
      [b.next(), b.next(), b.next(), b.next(), b.next()].should.deepEqual([100, 200, 400, 500, 500]);
      b.reset();
      b.next().should.equal(100);
    });
  });

  describe('formatDelay', function () {
    it('uses ms below one second and whole seconds above', function () {
      formatDelay(500).should.equal('500ms');
      formatDelay(1000).should.equal('1s');
      formatDelay(4000).should.equal('4s');
    });
  });

  describe('toKafkaHeaders', function () {
    it('returns undefined for missing/empty/non-object input', function () {
      should(toKafkaHeaders(undefined)).be.undefined();
      should(toKafkaHeaders({})).be.undefined();
      should(toKafkaHeaders('x')).be.undefined();
      should(toKafkaHeaders(['a'])).be.undefined();
    });

    it('coerces values to strings/buffers and drops null entries', function () {
      const buf = Buffer.from('b');
      toKafkaHeaders({
        s: 'x', n: 42, t: true, o: { a: 1 }, b: buf, nul: null, und: undefined, arr: ['a', 1, null]
      }).should.deepEqual({
        s: 'x', n: '42', t: 'true', o: '{"a":1}', b: buf, arr: ['a', '1']
      });
    });
  });
});
