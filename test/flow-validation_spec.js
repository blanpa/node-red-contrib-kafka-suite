'use strict';

const fs = require('fs');
const path = require('path');
const should = require('should');

describe('example flow: test-all-nodes.json', function () {
  const flowPath = path.join(__dirname, '..', 'examples', 'test-all-nodes.json');
  let flow;

  before(function () {
    const raw = fs.readFileSync(flowPath, 'utf8');
    flow = JSON.parse(raw);
  });

  it('is valid JSON and an array', function () {
    flow.should.be.an.Array();
    flow.length.should.be.greaterThan(0);
  });

  it('every node has a unique id', function () {
    const ids = flow.map(n => n.id);
    new Set(ids).size.should.equal(ids.length);
  });

  it('every non-config node references a valid tab (z)', function () {
    const tabIds = new Set(flow.filter(n => n.type === 'tab').map(n => n.id));
    const configTypes = new Set([
      'kafka-suite-broker',
      'kafka-suite-schema-registry'
    ]);
    flow.forEach(n => {
      if (n.type === 'tab' || configTypes.has(n.type)) return;
      should.exist(n.z, 'node ' + n.id + ' (' + n.type + ') is missing z');
      tabIds.has(n.z).should.equal(true, 'node ' + n.id + ' has unknown z=' + n.z);
    });
  });

  it('every wire target id exists', function () {
    const ids = new Set(flow.map(n => n.id));
    flow.forEach(n => {
      if (!Array.isArray(n.wires)) return;
      n.wires.forEach((out, i) => {
        out.forEach(tgt => {
          ids.has(tgt).should.equal(true,
            'wire from ' + n.id + '[' + i + '] → missing ' + tgt);
        });
      });
    });
  });

  it('broker references resolve to a kafka-suite-broker node', function () {
    const brokers = new Set(
      flow.filter(n => n.type === 'kafka-suite-broker').map(n => n.id)
    );
    flow.forEach(n => {
      if (n.broker !== undefined && n.broker !== '') {
        brokers.has(n.broker).should.equal(true,
          n.id + ' references unknown broker ' + n.broker);
      }
    });
  });

  it('uses only registered kafka-suite-* node types', function () {
    const allowedKafka = new Set([
      'kafka-suite-broker',
      'kafka-suite-producer',
      'kafka-suite-consumer',
      'kafka-suite-admin',
      'kafka-suite-schema-registry'
    ]);
    flow.forEach(n => {
      if (typeof n.type === 'string' && n.type.startsWith('kafka-')) {
        allowedKafka.has(n.type).should.equal(true,
          'unknown kafka node type: ' + n.type);
      }
    });
  });

  it('admin node has 1 output wired', function () {
    const admin = flow.find(n => n.type === 'kafka-suite-admin');
    should.exist(admin);
    admin.wires.length.should.equal(1);
  });

  it('producer node has 1 output wired', function () {
    const prod = flow.find(n => n.type === 'kafka-suite-producer');
    should.exist(prod);
    prod.wires.length.should.equal(1);
  });

  it('consumer node has 1 output wired', function () {
    const cons = flow.find(n => n.type === 'kafka-suite-consumer');
    should.exist(cons);
    cons.wires.length.should.equal(1);
  });

  it('includes a catch node for errors', function () {
    const catchNode = flow.find(n => n.type === 'catch');
    should.exist(catchNode);
  });

  it('every inject node carries the Node-RED 4 default fields', function () {
    // Without these the editor flags the node as "Invalid" with a red triangle
    // even though the runtime accepts it. We discovered this the hard way.
    const required = ['repeat', 'crontab', 'once', 'onceDelay', 'topic', 'props'];
    flow.filter(n => n.type === 'inject').forEach(n => {
      required.forEach(field => {
        should.notEqual(n[field], undefined,
          'inject ' + n.id + ' is missing required field "' + field + '"');
      });
      Array.isArray(n.props).should.equal(true,
        'inject ' + n.id + ' must have props as array');
    });
  });

  it('inject "json" props contain valid JSON literals', function () {
    flow.filter(n => n.type === 'inject').forEach(n => {
      n.props.forEach(p => {
        if (p.vt === 'json') {
          (function () { JSON.parse(p.v); }).should.not.throw(
            'inject ' + n.id + ' prop ' + p.p + ' has invalid JSON: ' + p.v);
        }
      });
    });
  });
});
