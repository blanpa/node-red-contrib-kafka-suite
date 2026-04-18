'use strict';

/**
 * HTML editor-defaults audit. We don't load the actual Node-RED editor —
 * we extract the registerType() defaults block via a minimal stub so that
 * regressions in field types (e.g. inputLabels: 'string' instead of array,
 * dropping required validators) get caught.
 */

const fs = require('fs');
const path = require('path');
const vm = require('vm');
const should = require('should');

function extractRegister(htmlPath) {
  const html = fs.readFileSync(htmlPath, 'utf8');
  // Grab the first <script type="text/javascript">…</script> block.
  const m = html.match(/<script type="text\/javascript">([\s\S]*?)<\/script>/);
  if (!m) throw new Error('no editor script in ' + htmlPath);
  const code = m[1];

  let captured = null;
  const sandbox = {
    RED: {
      nodes: {
        registerType: function (type, opts) { captured = { type, opts }; },
        node: function () { return null; }
      },
      validators: {
        number: function () { return function (v) { return !isNaN(parseFloat(v)); }; },
        regex: function (re) { return function (v) { return re.test(v); }; }
      }
    },
    $: function () {
      const chain = { on: function () { return chain; }, val: function () { return ''; }, hide: function () { return chain; }, show: function () { return chain; }, find: function () { return chain; }, toggleClass: function () { return chain; }, toggle: function () { return chain; }, text: function () { return chain; }, is: function () { return false; } };
      return chain;
    }
  };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  if (!captured) throw new Error('registerType() not invoked in ' + htmlPath);
  return captured.opts;
}

const root = path.join(__dirname, '..', 'nodes');

describe('HTML editor defaults', function () {
  describe('kafka-suite-broker', function () {
    const opts = extractRegister(path.join(root, 'kafka-suite-broker.html'));

    it('declares all expected default fields', function () {
      const expected = ['name', 'brokers', 'clientId', 'backend', 'servicePreset',
        'authType', 'logLevel', 'connectionTimeout', 'requestTimeout',
        'maxRetryTime', 'initialRetryTime', 'retries', 'sslRejectUnauthorized'];
      expected.forEach(k => should.exist(opts.defaults[k], 'missing default: ' + k));
    });

    it('marks brokers as required and validates host:port format', function () {
      opts.defaults.brokers.required.should.equal(true);
      const v = opts.defaults.brokers.validate;
      v('localhost:9092').should.equal(true);
      v('broker-1:9092,broker-2:9093').should.equal(true);
      v('not a broker').should.equal(false);
      v('').should.equal(false);
      v('missing-port').should.equal(false);
    });

    it('declares credentials block (so secrets do not export with flows)', function () {
      should.exist(opts.credentials);
      opts.credentials.password.type.should.equal('password');
      opts.credentials.sslKey.type.should.equal('password');
    });
  });

  describe('kafka-suite-producer', function () {
    const opts = extractRegister(path.join(root, 'kafka-suite-producer.html'));

    it('exposes a kafka-svg icon and palette label', function () {
      opts.icon.should.equal('kafka.svg');
      opts.paletteLabel.should.equal('kafka producer');
    });

    it('inputLabels and outputLabels are arrays', function () {
      Array.isArray(opts.inputLabels).should.equal(true);
      Array.isArray(opts.outputLabels).should.equal(true);
    });

    it('partition validator accepts numeric or empty', function () {
      const v = opts.defaults.partition.validate;
      v('').should.equal(true);
      v('0').should.equal(true);
      v('17').should.equal(true);
      v('abc').should.equal(false);
      v('-1').should.equal(false);
    });

    it('topic validator accepts kafka-legal characters', function () {
      const v = opts.defaults.topic.validate;
      v('').should.equal(true); // can come from msg.topic
      v('my.topic_v1-final').should.equal(true);
      v('with spaces').should.equal(false);
      v('with/slash').should.equal(false);
    });

    it('schemaRegistry validator handles empty + _ADD_', function () {
      const v = opts.defaults.schemaRegistry.validate;
      v('').should.equal(true);
      v('_ADD_').should.equal(true);
    });
  });

  describe('kafka-suite-consumer', function () {
    const opts = extractRegister(path.join(root, 'kafka-suite-consumer.html'));

    it('inputLabels MUST be an array (was a string bug)', function () {
      Array.isArray(opts.inputLabels).should.equal(true,
        'inputLabels as a plain string breaks Node-RED port tooltips');
      opts.inputLabels.length.should.equal(1);
    });

    it('topics validator splits on comma and validates each', function () {
      const v = opts.defaults.topics.validate;
      v('one').should.equal(true);
      v('one,two,three').should.equal(true);
      v('one, two, three').should.equal(true);
      v('').should.equal(false);
      v('bad name').should.equal(false);
      v('one,bad name').should.equal(false);
    });

    it('autoCommit defaults to true so newcomers do not lose messages', function () {
      opts.defaults.autoCommit.value.should.equal(true);
    });
  });

  describe('kafka-suite-admin', function () {
    const opts = extractRegister(path.join(root, 'kafka-suite-admin.html'));

    it('inputLabels and outputLabels are arrays', function () {
      Array.isArray(opts.inputLabels).should.equal(true);
      Array.isArray(opts.outputLabels).should.equal(true);
    });

    it('only requires a broker — actions come from msg.action', function () {
      opts.defaults.broker.required.should.equal(true);
      Object.keys(opts.defaults).length.should.equal(2); // name + broker
    });
  });

  describe('kafka-suite-schema-registry', function () {
    const opts = extractRegister(path.join(root, 'kafka-suite-schema-registry.html'));

    it('registryUrl must be required and validate http(s) URLs', function () {
      opts.defaults.registryUrl.required.should.equal(true);
      const v = opts.defaults.registryUrl.validate;
      v('http://localhost:8081').should.equal(true);
      v('https://psrc-xyz.aws.confluent.cloud').should.equal(true);
      v('localhost:8081').should.equal(false);
      v('').should.equal(false);
    });

    it('credentials block exists for API Key/Secret', function () {
      should.exist(opts.credentials);
      opts.credentials.password.type.should.equal('password');
    });
  });

  it('kafka.svg icon file actually exists on disk', function () {
    fs.existsSync(path.join(root, 'icons', 'kafka.svg')).should.equal(true);
  });
});
