'use strict';

// KafkaJS only ships GZIP. Snappy, LZ4 and ZSTD must be registered as
// external codecs — without them the consumer crashes the moment it
// encounters a batch produced with one of those codecs (regardless of
// whether *we* asked to produce with compression). See issue #1.
//
// Codecs are registered eagerly when this module is first required so the
// shared kafkajs `CompressionCodecs` table is populated before any client
// instance is built. Missing packages are tolerated — the producer surfaces
// a clear error if the user actually selects an unavailable codec.

const { CompressionTypes, CompressionCodecs } = require('kafkajs');

const REGISTRATIONS = [
  {
    name: 'snappy',
    type: CompressionTypes.Snappy,
    pkg: 'kafkajs-snappy',
    build: (mod) => mod
  },
  {
    name: 'lz4',
    type: CompressionTypes.LZ4,
    pkg: 'kafkajs-lz4',
    build: (mod) => new mod().codec
  },
  {
    name: 'zstd',
    type: CompressionTypes.ZSTD,
    pkg: '@kafkajs/zstd',
    build: (mod) => mod()
  }
];

const status = {};

for (const reg of REGISTRATIONS) {
  try {
    const mod = require(reg.pkg);
    CompressionCodecs[reg.type] = reg.build(mod);
    status[reg.name] = { available: true, pkg: reg.pkg };
  } catch (e) {
    status[reg.name] = { available: false, pkg: reg.pkg, error: e.message };
  }
}

function isAvailable(name) {
  // gzip and none are always built into kafkajs
  if (name === 'none' || name === 'gzip') return true;
  return !!(status[name] && status[name].available);
}

function packageFor(name) {
  return status[name] ? status[name].pkg : null;
}

module.exports = { isAvailable, packageFor, status };
