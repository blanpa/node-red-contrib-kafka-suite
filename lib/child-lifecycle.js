'use strict';

const { Backoff, formatDelay } = require('./util');

const DEFAULT_INITIAL_RETRY = 1000;
const DEFAULT_MAX_RETRY = 30000;

/**
 * Drives the Kafka resource of a child node (producer, consumer or admin
 * client) through the broker connection lifecycle:
 *
 *   - start()   the broker is connected: create and start the resource
 *               (called by the broker node, or on register when the broker
 *               is already connected).
 *   - restart() tear the resource down and try again after a backoff delay
 *               (e.g. after a consumer crashed for good).
 *   - stop()    tear the resource down without retrying (node closing).
 *
 * A failed start is torn down and retried with exponential backoff, so a
 * persistent error (missing topic, ACL denied, ...) is logged at most once
 * per backoff interval instead of on a fixed fast poll.
 *
 * Hooks:
 *   create(client)    synchronously create the resource from the broker client
 *   start(resource)   async; connect/subscribe/run; must throw on failure
 *   attach(resource)  resource is up: expose it on the node, set status
 *   detach()          resource is gone: clear it from the node
 */
class ChildLifecycle {
  constructor(node, { label, create, start, attach, detach, initialRetryTime, maxRetryTime }) {
    this.node = node;
    this.label = label;
    this.hooks = { create, start, attach, detach };
    this.backoff = new Backoff(initialRetryTime || DEFAULT_INITIAL_RETRY, maxRetryTime || DEFAULT_MAX_RETRY);
    this.resource = null;
    this.active = false;
    this.timer = null;
    // Bumped by restart()/stop() so an in-flight start can tell it has been
    // superseded and must discard its resource instead of attaching it.
    this.generation = 0;
    this.startingGeneration = null;
  }

  start() {
    this.active = true;
    this._clearTimer();
    return this._attempt();
  }

  restart() {
    const gen = ++this.generation;
    this._clearTimer();
    return this._teardown().then(() => {
      if (gen === this.generation && this.active) this._schedule();
    });
  }

  async stop() {
    this.active = false;
    this.generation++;
    this._clearTimer();
    await this._teardown();
  }

  async _attempt() {
    this.timer = null;
    if (!this.active || this.resource || this.startingGeneration === this.generation) return;
    const broker = this.node.brokerNode;
    const client = broker && broker.connected ? broker.getClient() : null;
    // Not connected yet: the broker calls start() again once it is.
    if (!client) return;

    const gen = this.generation;
    this.startingGeneration = gen;
    let resource = null;
    try {
      resource = this.hooks.create(client);
      this.resource = resource;
      await this.hooks.start(resource);
      if (gen !== this.generation) throw new Error('superseded');
      this.backoff.reset();
      this.hooks.attach(resource);
    } catch (err) {
      if (gen !== this.generation) {
        // restart()/stop() already tore down this.resource; make sure the
        // resource of this superseded attempt is not left running.
        await quietDisconnect(resource);
        return;
      }
      await this._teardown();
      if (!this.active) return;
      const delay = formatDelay(this._schedule());
      this.node.status({ fill: 'red', shape: 'ring', text: this.label + ' error (retry in ' + delay + ')' });
      this.node.error('Failed to start ' + this.label + ': ' + err.message + ' (retrying in ' + delay + ')');
    } finally {
      if (this.startingGeneration === gen) this.startingGeneration = null;
    }
  }

  _schedule() {
    const delay = this.backoff.next();
    this._clearTimer();
    this.timer = setTimeout(() => this._attempt(), delay);
    return delay;
  }

  _clearTimer() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  async _teardown() {
    const resource = this.resource;
    this.resource = null;
    this.hooks.detach();
    await quietDisconnect(resource);
  }
}

async function quietDisconnect(resource) {
  if (!resource || typeof resource.disconnect !== 'function') return;
  try { await resource.disconnect(); } catch (e) { /* ignore */ }
}

module.exports = { ChildLifecycle };
