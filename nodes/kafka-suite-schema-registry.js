'use strict';

const { intOr } = require('../lib/util');

module.exports = function (RED) {
  function KafkaSchemaRegistryNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.registryUrl = config.registryUrl || 'http://localhost:8081';
    node.schemaType = config.schemaType || 'AVRO';
    node.cacheTTL = intOr(config.cacheTTL, 300000, 0); // 5 minutes
    node.autoRegister = config.autoRegister || false;

    node.registry = null;
    node.libraryAvailable = false;
    node._schemaCache = {};
    // subject + type + schema text -> schema id. A registered schema's id
    // never changes, so repeated registerSchema() calls with the same
    // definition (e.g. msg.schemaDefinition on every message) skip the
    // HTTP round-trip.
    node._registeredCache = new Map();

    // Try to load schema registry library
    node._initRegistry = function () {
      try {
        const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
        const registryConfig = { host: node.registryUrl };
        const creds = node.credentials || {};
        if (creds.username && creds.password) {
          registryConfig.auth = {
            username: creds.username,
            password: creds.password
          };
        }
        node.registry = new SchemaRegistry(registryConfig);
        node.SchemaType = SchemaType;
        node.libraryAvailable = true;
        node.log('Schema Registry configured: ' + node.registryUrl);
        return true;
      } catch (err) {
        node.libraryAvailable = false;
        node.warn(
          'Schema Registry library not available. ' +
          'Install with: npm install @kafkajs/confluent-schema-registry'
        );
        return false;
      }
    };

    node._initRegistry();

    /**
     * Encode a payload using the schema registry. Uses the given schema id,
     * or the latest schema registered for the subject.
     */
    node.encode = async function (subject, payload, schemaIdOverride) {
      if (!node.registry) {
        throw new Error('Schema Registry not initialized');
      }
      if (schemaIdOverride != null) {
        return await node.registry.encode(schemaIdOverride, payload);
      }

      // Get or cache schema ID
      let schemaId = node._schemaCache[subject];
      if (!schemaId || Date.now() - schemaId._cachedAt > node.cacheTTL) {
        const latestSchema = await node.registry.getLatestSchemaId(subject);
        schemaId = { id: latestSchema, _cachedAt: Date.now() };
        node._schemaCache[subject] = schemaId;
      }

      return await node.registry.encode(schemaId.id, payload);
    };

    /**
     * Decode a buffer using the schema registry
     */
    node.decode = async function (buffer) {
      if (!node.registry) {
        throw new Error('Schema Registry not initialized');
      }
      return await node.registry.decode(buffer);
    };

    /**
     * Get schema by subject and version
     */
    node.getSchema = async function (subject, version) {
      if (!node.registry) {
        throw new Error('Schema Registry not initialized');
      }
      if (version === 'latest') {
        return await node.registry.getLatestSchemaId(subject);
      }
      return await node.registry.getRegistryId(subject, version);
    };

    /**
     * Register (or update) a schema for a subject. The schema is either an
     * object (Avro/JSON Schema) or a string (Protobuf IDL). The result is the
     * numeric schema id and is cached so subsequent encode() calls skip the
     * lookup round-trip.
     */
    node.registerSchema = async function (subject, schema, type) {
      if (!node.registry) {
        throw new Error('Schema Registry not initialized');
      }
      const schemaType = (type || node.schemaType || 'AVRO').toUpperCase();
      const schemaText = typeof schema === 'string' ? schema : JSON.stringify(schema);
      const cacheKey = subject + '\0' + schemaType + '\0' + schemaText;
      if (node._registeredCache.has(cacheKey)) {
        return node._registeredCache.get(cacheKey);
      }
      const payload = {
        type: node.SchemaType[schemaType],
        schema: schemaText
      };
      const { id } = await node.registry.register(payload, { subject });
      node._registeredCache.set(cacheKey, id);
      // Prime the cache so the very next encode() doesn't re-fetch
      node._schemaCache[subject] = { id, _cachedAt: Date.now() };
      return id;
    };

    node.on('close', function (done) {
      node.registry = null;
      node._schemaCache = {};
      node._registeredCache.clear();
      done();
    });
  }

  RED.nodes.registerType('kafka-suite-schema-registry', KafkaSchemaRegistryNode, {
    credentials: {
      username: { type: 'text' },
      password: { type: 'password' }
    }
  });
};
