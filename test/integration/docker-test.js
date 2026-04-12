'use strict';

/**
 * Integration test that runs against a local Kafka broker.
 * Requires: docker compose up -d kafka (just kafka, no node-red needed)
 *
 * Tests:
 * 1. Adapter connects to broker
 * 2. Admin: create topic, list topics, describe cluster
 * 3. Producer: send single message, send batch
 * 4. Consumer: receive messages with correct metadata
 * 5. Admin: fetch offsets, list groups
 * 6. Cleanup: delete topic
 */

const KafkaJSAdapter = require('../../lib/adapters/kafkajs-adapter');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TEST_TOPIC = 'kafka-suite-integration-test-' + Date.now();
const GROUP_ID = 'test-group-' + Date.now();

let adapter, admin, producer, consumer;
let receivedMessages = [];
let testsPassed = 0;
let testsFailed = 0;

function assert(condition, testName) {
  if (condition) {
    console.log('  PASS: ' + testName);
    testsPassed++;
  } else {
    console.log('  FAIL: ' + testName);
    testsFailed++;
  }
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTests() {
  console.log('\n=== Kafka Suite Integration Tests ===');
  console.log('Broker: ' + BROKER);
  console.log('Test Topic: ' + TEST_TOPIC);
  console.log('');

  try {
    // 1. Connect
    console.log('--- 1. Connection ---');
    adapter = new KafkaJSAdapter({
      clientId: 'integration-test',
      brokers: [BROKER],
      connectionTimeout: 10000,
      requestTimeout: 30000,
      logLevel: 'error',
      retries: 3,
      maxRetryTime: 30000,
      initialRetryTime: 300
    });
    await adapter.connect();
    assert(adapter.isConnected(), 'Adapter connects successfully');

    // 2. Admin operations
    console.log('\n--- 2. Admin Operations ---');
    admin = adapter.createAdmin();
    await admin.connect();
    assert(true, 'Admin connects');

    // Create topic
    const created = await admin.createTopics([{
      topic: TEST_TOPIC,
      numPartitions: 2,
      replicationFactor: 1
    }]);
    assert(created === true || created === false, 'Create topic completes');

    await sleep(1000); // Wait for topic to be ready

    // List topics
    const topics = await admin.listTopics();
    assert(Array.isArray(topics) && topics.includes(TEST_TOPIC), 'List topics contains test topic');

    // Describe cluster
    const cluster = await admin.describeCluster();
    assert(cluster && cluster.brokers && cluster.brokers.length > 0, 'Describe cluster returns brokers');
    console.log('    Cluster: ' + cluster.brokers.length + ' broker(s), controller: ' + cluster.controller);

    // 3. Producer
    console.log('\n--- 3. Producer ---');
    producer = adapter.createProducer({});
    await producer.connect();
    assert(true, 'Producer connects');

    // Send single message
    const singleResult = await producer.send({
      topic: TEST_TOPIC,
      messages: [{
        key: 'test-key-1',
        value: { sensor: 'temp-1', value: 23.5, unit: 'celsius' },
        headers: { source: 'integration-test' }
      }],
      acks: -1,
      timeout: 30000,
      compression: 'none'
    });
    assert(singleResult && singleResult[0], 'Single message sent successfully');
    console.log('    Partition: ' + singleResult[0].partition + ', Offset: ' + singleResult[0].baseOffset);

    // Send batch
    const batchResult = await producer.send({
      topic: TEST_TOPIC,
      messages: [
        { key: 'batch-1', value: { id: 1, msg: 'batch message 1' } },
        { key: 'batch-2', value: { id: 2, msg: 'batch message 2' } },
        { key: 'batch-3', value: { id: 3, msg: 'batch message 3' } }
      ],
      acks: -1,
      timeout: 30000,
      compression: 'none'
    });
    assert(batchResult && batchResult.length > 0, 'Batch messages sent successfully');

    // 4. Consumer
    console.log('\n--- 4. Consumer ---');
    consumer = adapter.createConsumer(GROUP_ID, {
      sessionTimeout: 30000,
      heartbeatInterval: 3000
    });
    await consumer.connect();
    assert(true, 'Consumer connects');

    await consumer.subscribe({
      topics: [TEST_TOPIC],
      fromBeginning: true
    });

    // Run consumer and collect messages
    const consumePromise = new Promise((resolve) => {
      const timeout = setTimeout(() => resolve(), 15000);
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          receivedMessages.push({
            topic,
            partition,
            key: message.key ? message.key.toString() : null,
            value: message.value ? message.value.toString() : null,
            offset: message.offset,
            headers: message.headers
          });
          // We sent 4 messages total
          if (receivedMessages.length >= 4) {
            clearTimeout(timeout);
            setTimeout(() => resolve(), 1000);
          }
        },
        autoCommit: true,
        autoCommitInterval: 1000,
        partitionsConsumedConcurrently: 1
      });
    });

    await consumePromise;

    assert(receivedMessages.length >= 4, 'Consumer received all ' + receivedMessages.length + '/4 messages');

    // Check message content
    const singleMsg = receivedMessages.find(m => m.key === 'test-key-1');
    if (singleMsg) {
      const parsed = JSON.parse(singleMsg.value);
      assert(parsed.sensor === 'temp-1' && parsed.value === 23.5, 'Single message content is correct');
      assert(singleMsg.headers && singleMsg.headers.source, 'Message headers preserved');
    } else {
      assert(false, 'Single message found in received messages');
    }

    const batchMsgs = receivedMessages.filter(m => m.key && m.key.startsWith('batch-'));
    assert(batchMsgs.length === 3, 'All 3 batch messages received');

    // 5. Admin: post-consume checks
    console.log('\n--- 5. Post-Consume Admin Checks ---');
    const offsets = await admin.fetchTopicOffsets(TEST_TOPIC);
    assert(Array.isArray(offsets) && offsets.length === 2, 'Fetch offsets returns 2 partitions');
    console.log('    Offsets: ' + JSON.stringify(offsets.map(o => ({ p: o.partition, high: o.high, low: o.low }))));

    const groups = await admin.listGroups();
    assert(groups && groups.groups, 'List groups returns result');

    // 6. Cleanup
    console.log('\n--- 6. Cleanup ---');
    await consumer.disconnect();
    assert(true, 'Consumer disconnected');

    await producer.disconnect();
    assert(true, 'Producer disconnected');

    await admin.deleteTopics([TEST_TOPIC]);
    assert(true, 'Test topic deleted');

    await admin.disconnect();
    assert(true, 'Admin disconnected');

    await adapter.disconnect();
    assert(true, 'Adapter disconnected');

  } catch (err) {
    console.error('\n  ERROR: ' + err.message);
    console.error(err.stack);
    testsFailed++;
  } finally {
    // Ensure cleanup
    try { if (consumer) await consumer.disconnect(); } catch (e) {}
    try { if (producer) await producer.disconnect(); } catch (e) {}
    try { if (admin) await admin.disconnect(); } catch (e) {}
    try { if (adapter) await adapter.disconnect(); } catch (e) {}
  }

  console.log('\n=== Results ===');
  console.log('Passed: ' + testsPassed);
  console.log('Failed: ' + testsFailed);
  console.log('');

  process.exit(testsFailed > 0 ? 1 : 0);
}

runTests();
