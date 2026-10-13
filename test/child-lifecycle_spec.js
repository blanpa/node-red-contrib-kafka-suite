'use strict';

const should = require('should');
const { ChildLifecycle } = require('../lib/child-lifecycle');

describe('ChildLifecycle', function () {
  function makeNode(connected) {
    const node = {
      statuses: [],
      errors: [],
      attached: null,
      status: function (s) { node.statuses.push(s); },
      error: function (e) { node.errors.push(e); },
      brokerNode: {
        connected: connected !== false,
        getClient: function () { return {}; }
      }
    };
    return node;
  }

  function makeResource(opts) {
    opts = opts || {};
    const r = {
      disconnected: 0,
      disconnect: async function () { r.disconnected++; }
    };
    r.start = opts.start || (async () => {});
    return r;
  }

  function makeLifecycle(node, resources, extra) {
    let i = 0;
    return new ChildLifecycle(node, Object.assign({
      label: 'thing',
      create: () => resources[Math.min(i++, resources.length - 1)],
      start: (r) => r.start(),
      attach: (r) => { node.attached = r; },
      detach: () => { node.attached = null; },
      initialRetryTime: 20,
      maxRetryTime: 40
    }, extra || {}));
  }

  it('does nothing while the broker is not connected', async function () {
    const node = makeNode(false);
    let created = 0;
    const lc = makeLifecycle(node, [makeResource()], { create: () => { created++; return makeResource(); } });
    await lc.start();
    created.should.equal(0);
    should(node.attached).be.null();
  });

  it('attaches the resource after a successful start', async function () {
    const node = makeNode();
    const r = makeResource();
    const lc = makeLifecycle(node, [r]);
    await lc.start();
    node.attached.should.equal(r);
    lc.resource.should.equal(r);
  });

  it('tears down a failed start and retries with backoff', async function () {
    const node = makeNode();
    const bad = makeResource({ start: async () => { throw new Error('topic missing'); } });
    const good = makeResource();
    const lc = makeLifecycle(node, [bad, good]);
    await lc.start();
    bad.disconnected.should.equal(1);
    should(node.attached).be.null();
    node.errors.length.should.equal(1);
    String(node.errors[0]).should.match(/topic missing/);
    node.statuses[node.statuses.length - 1].fill.should.equal('red');
    await new Promise(resolve => setTimeout(resolve, 40));
    node.attached.should.equal(good);
    await lc.stop();
  });

  it('does not log on every fast tick: retry delays grow', async function () {
    const node = makeNode();
    const bad = makeResource({ start: async () => { throw new Error('denied'); } });
    const lc = makeLifecycle(node, [bad], { initialRetryTime: 30, maxRetryTime: 1000 });
    await lc.start();
    // attempts at t=0, 30, 90 (delays 30, 60, ...)
    await new Promise(resolve => setTimeout(resolve, 110));
    await lc.stop();
    node.errors.length.should.be.within(2, 3);
  });

  it('restart() tears down and starts a fresh resource', async function () {
    const node = makeNode();
    const first = makeResource();
    const second = makeResource();
    const lc = makeLifecycle(node, [first, second]);
    await lc.start();
    await lc.restart();
    first.disconnected.should.equal(1);
    should(node.attached).be.null();
    await new Promise(resolve => setTimeout(resolve, 40));
    node.attached.should.equal(second);
    await lc.stop();
  });

  it('stop() during an in-flight start discards the resource', async function () {
    const node = makeNode();
    let release;
    const slow = makeResource({ start: () => new Promise(resolve => { release = resolve; }) });
    const lc = makeLifecycle(node, [slow]);
    const pending = lc.start();
    await lc.stop();
    release();
    await pending;
    should(node.attached).be.null();
    slow.disconnected.should.be.aboveOrEqual(1);
    node.errors.length.should.equal(0);
  });
});
