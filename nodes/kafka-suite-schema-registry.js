'use strict';

module.exports = function (RED) {
  function KafkaSchemaRegistryNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.registryUrl = config.registryUrl || 'http://localhost:8081';
    node.schemaType = config.schemaType || 'AVRO';
    node.cacheTTL = parseInt(config.cacheTTL) || 300000; // 5 minutes
    node.autoRegister = config.autoRegister || false;

    node.registry = null;
    node.libraryAvailable = false;
    node._schemaCache = {};

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
     * Encode a payload using the schema registry
     */
    node.encode = async function (subject, payload) {
      if (!node.registry) {
        throw new Error('Schema Registry not initialized');
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
      const payload = {
        type: node.SchemaType[schemaType],
        schema: typeof schema === 'string' ? schema : JSON.stringify(schema)
      };
      const { id } = await node.registry.register(payload, { subject });
      // Prime the cache so the very next encode() doesn't re-fetch
      node._schemaCache[subject] = { id, _cachedAt: Date.now() };
      return id;
    };

    node.on('close', function (done) {
      node.registry = null;
      node._schemaCache = {};
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
