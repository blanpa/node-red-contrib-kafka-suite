'use strict';

const should = require('should');
const helper = require('node-red-node-test-helper');
const registryNode = require('../../nodes/kafka-suite-schema-registry.js');

helper.init(require.resolve('node-red'));

describe('kafka-suite-schema-registry node', function () {
  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) { helper.unload().then(() => helper.stopServer(done)); });

  it('loads with defaults', function (done) {
    const flow = [{
      id: 'r1', type: 'kafka-suite-schema-registry',
      registryUrl: 'http://schema:8081', schemaType: 'AVRO'
    }];
    helper.load(registryNode, flow, function () {
      const r = helper.getNode('r1');
      should.exist(r);
      r.registryUrl.should.equal('http://schema:8081');
      r.schemaType.should.equal('AVRO');
      r.cacheTTL.should.equal(300000);
      done();
    });
  });

  it('uses default URL when none configured', function (done) {
    const flow = [{ id: 'r1', type: 'kafka-suite-schema-registry' }];
    helper.load(registryNode, flow, function () {
      const r = helper.getNode('r1');
      r.registryUrl.should.equal('http://localhost:8081');
      done();
    });
  });

  it('parses cacheTTL as integer', function (done) {
    const flow = [{
      id: 'r1', type: 'kafka-suite-schema-registry', cacheTTL: '60000'
    }];
    helper.load(registryNode, flow, function () {
      const r = helper.getNode('r1');
      r.cacheTTL.should.equal(60000);
      done();
    });
  });

  describe('encode/decode (mocked registry)', function () {
    function loadAndStub(cb) {
      const flow = [{ id: 'r1', type: 'kafka-suite-schema-registry' }];
      helper.load(registryNode, flow, function () {
        const r = helper.getNode('r1');
        cb(r);
      });
    }

    it('encode throws when registry not initialized', function (done) {
      loadAndStub(async (r) => {
        r.registry = null;
        try {
          await r.encode('subj', { a: 1 });
          done(new Error('should have thrown'));
        } catch (e) {
          e.message.should.match(/not initialized/);
          done();
        }
      });
    });

    it('decode throws when registry not initialized', function (done) {
      loadAndStub(async (r) => {
        r.registry = null;
        try {
          await r.decode(Buffer.from([0]));
          done(new Error('should have thrown'));
        } catch (e) {
          e.message.should.match(/not initialized/);
          done();
        }
      });
    });

    it('encode caches schema id and reuses on next call', function (done) {
      loadAndStub(async (r) => {
        let getLatestCalls = 0, encodeCalls = 0;
        r.registry = {
          getLatestSchemaId: async function () { getLatestCalls++; return 42; },
          encode: async function (id, payload) { encodeCalls++; return Buffer.from([0, 0, 0, 0, id]); }
        };
        await r.encode('subj-a', { a: 1 });
        await r.encode('subj-a', { a: 2 });
        await r.encode('subj-a', { a: 3 });
        getLatestCalls.should.equal(1, 'schema lookup should be cached');
        encodeCalls.should.equal(3);
        done();
      });
    });

    it('encode refetches schema id after TTL expires', function (done) {
      const flow = [{
        id: 'r1', type: 'kafka-suite-schema-registry', cacheTTL: '50'
      }];
      helper.load(registryNode, flow, async function () {
        const r = helper.getNode('r1');
        let getLatestCalls = 0;
        r.registry = {
          getLatestSchemaId: async function () { getLatestCalls++; return 7; },
          encode: async function () { return Buffer.from([]); }
        };
        await r.encode('subj', { a: 1 });
        await new Promise(res => setTimeout(res, 80));
        await r.encode('subj', { a: 2 });
        getLatestCalls.should.equal(2);
        done();
      });
    });

    it('decode delegates to registry.decode()', function (done) {
      loadAndStub(async (r) => {
        r.registry = {
          decode: async function (buf) { return { length: buf.length, ok: true }; }
        };
        const result = await r.decode(Buffer.from([0, 0, 0, 0, 1, 65]));
        result.ok.should.equal(true);
        result.length.should.equal(6);
        done();
      });
    });

    it('getSchema(version=latest) calls getLatestSchemaId', function (done) {
      loadAndStub(async (r) => {
        let called = false;
        r.registry = {
          getLatestSchemaId: async function () { called = true; return 11; },
          getRegistryId: async function () { throw new Error('should not'); }
        };
        const id = await r.getSchema('subj', 'latest');
        id.should.equal(11);
        called.should.equal(true);
        done();
      });
    });

    it('registerSchema throws when registry not initialized', function (done) {
      loadAndStub(async (r) => {
        r.registry = null;
        try {
          await r.registerSchema('s', { type: 'record' });
          done(new Error('should have thrown'));
        } catch (e) {
          e.message.should.match(/not initialized/);
          done();
        }
      });
    });

    it('registerSchema posts schema and primes the cache', function (done) {
      loadAndStub(async (r) => {
        let registerCalledWith = null;
        r.SchemaType = { AVRO: 'AVRO', JSON: 'JSON', PROTOBUF: 'PROTOBUF' };
        r.registry = {
          register: async function (payload, opts) {
            registerCalledWith = { payload, opts };
            return { id: 77 };
          },
          getLatestSchemaId: async function () { throw new Error('should not refetch'); },
          encode: async function (id) { return Buffer.from([0, 0, 0, 0, id]); }
        };
        const schema = { type: 'record', name: 'Sensor', fields: [{ name: 'v', type: 'double' }] };
        const id = await r.registerSchema('sensor-events-value', schema);
        id.should.equal(77);
        registerCalledWith.opts.subject.should.equal('sensor-events-value');
        registerCalledWith.payload.type.should.equal('AVRO');
        // Schema must be sent as JSON string
        JSON.parse(registerCalledWith.payload.schema).name.should.equal('Sensor');
        // Cache primed: encode() should NOT call getLatestSchemaId
        await r.encode('sensor-events-value', { v: 1.0 });
        done();
      });
    });

    it('registerSchema accepts string schemas (Protobuf IDL)', function (done) {
      loadAndStub(async (r) => {
        let registerCalledWith = null;
        r.SchemaType = { AVRO: 'AVRO', PROTOBUF: 'PROTOBUF' };
        r.registry = {
          register: async function (payload) {
            registerCalledWith = payload;
            return { id: 5 };
          }
        };
        const idl = 'syntax = "proto3"; message M { string n = 1; }';
        await r.registerSchema('sub', idl, 'PROTOBUF');
        registerCalledWith.type.should.equal('PROTOBUF');
        registerCalledWith.schema.should.equal(idl);
        done();
      });
    });

    it('getSchema(version=N) calls getRegistryId', function (done) {
      loadAndStub(async (r) => {
        let calledWith = null;
        r.registry = {
          getLatestSchemaId: async function () { throw new Error('should not'); },
          getRegistryId: async function (s, v) { calledWith = { s, v }; return 22; }
        };
        const id = await r.getSchema('subj', 3);
        id.should.equal(22);
        calledWith.should.deepEqual({ s: 'subj', v: 3 });
        done();
      });
    });
  });
});
