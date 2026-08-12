const assert = require('node:assert/strict');
const { EventEmitter, once } = require('node:events');
const test = require('node:test');

const jwt = require('jsonwebtoken');
const { io: createClient } = require('socket.io-client');
const { createRealtimeServer, deriveHmacKey } = require('../index');

class FakeRedis extends EventEmitter {
  constructor() {
    super();
    this.hashes = new Map();
    this.sets = new Map();
    this.calls = [];
  }
  subscribe(_channel, callback) { callback(null, 1); }
  quit() { return Promise.resolve(); }
  hmget(key, ...fields) {
    const hash = this.hashes.get(key) || {};
    return Promise.resolve(fields.map((field) => hash[field] ?? null));
  }
  hget(key, field) { return Promise.resolve((this.hashes.get(key) || {})[field] ?? null); }
  hgetall(key) { return Promise.resolve({ ...(this.hashes.get(key) || {}) }); }
  hset(key, values) {
    this.hashes.set(key, { ...(this.hashes.get(key) || {}), ...values });
    return Promise.resolve(1);
  }
  sadd(key, ...members) {
    const set = this.sets.get(key) || new Set();
    members.forEach((member) => set.add(String(member)));
    this.sets.set(key, set);
    return Promise.resolve(members.length);
  }
  srem(key, ...members) {
    const set = this.sets.get(key) || new Set();
    members.forEach((member) => set.delete(String(member)));
    return Promise.resolve(members.length);
  }
  sismember(key, member) {
    return Promise.resolve(this.sets.get(key)?.has(String(member)) ? 1 : 0);
  }
  pipeline() {
    const self = this;
    const pipeline = {};
    for (const method of ['hset', 'sadd', 'srem']) {
      pipeline[method] = (...args) => { self.calls.push([method, ...args]); self[method](...args); return pipeline; };
    }
    for (const method of ['expire', 'pexpire', 'geoadd', 'del']) {
      pipeline[method] = (...args) => {
        self.calls.push([method, ...args]);
        if (method === 'del') self.hashes.delete(args[0]);
        return pipeline;
      };
    }
    pipeline.exec = () => Promise.resolve([]);
    return pipeline;
  }
}

function accessToken(secret, role = 'ADMIN', subject = 'admin-1') {
  return jwt.sign({ type: 'access', role }, deriveHmacKey(secret), {
    algorithm: 'HS256',
    subject,
    expiresIn: '5m',
  });
}

async function fixture(t, redis = new FakeRedis()) {
  const secret = 'a-test-access-secret-with-enough-entropy';
  const realtime = createRealtimeServer({
    env: {
      PORT: '0',
      HOST: '127.0.0.1',
      JWT_ACCESS_SECRET: secret,
      REALTIME_INTERNAL_SECRET: 'internal-test-secret',
    },
    redis,
    sub: new FakeRedis(),
  });
  const address = await realtime.start();
  const baseUrl = `http://127.0.0.1:${address.port}`;
  t.after(() => realtime.stop());
  return { realtime, baseUrl, secret, redis };
}

test('health endpoint reports the Redis subscription and rejects anonymous sockets', async (t) => {
  const { baseUrl } = await fixture(t);
  const health = await fetch(`${baseUrl}/health`).then((response) => response.json());
  assert.equal(health.ok, true);
  assert.equal(health.redisSubHealthy, true);

  const client = createClient(baseUrl, { transports: ['websocket'], reconnection: false });
  t.after(() => client.close());
  const [error] = await once(client, 'connect_error');
  assert.equal(error.message, 'AUTH_REQUIRED');
});

test('ADMIN, KYC, and SUPPORT users receive deduplicated KYC review events', async (t) => {
  const { realtime, baseUrl, secret } = await fixture(t);
  const client = createClient(baseUrl, {
    transports: ['websocket'],
    reconnection: false,
    auth: { token: accessToken(secret, 'KYC') },
  });
  t.after(() => client.close());
  await once(client, 'ready');

  const received = once(client, 'kyc.request_submitted');
  const event = {
    type: 'kyc.request_submitted',
    eventId: 'same-event-id',
    payload: { kycId: 'kyc-1', helperId: 'helper-1' },
  };
  realtime.emitEvent(event, 'redis');
  realtime.emitEvent(event, 'http');
  const [payload] = await received;
  assert.equal(payload.kycId, 'kyc-1');

  let duplicates = 0;
  client.on('kyc.request_submitted', () => { duplicates += 1; });
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(duplicates, 0);
});

test('internal publish requires its separate secret', async (t) => {
  const { baseUrl } = await fixture(t);
  const unauthorized = await fetch(`${baseUrl}/internal/publish`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'task.created', payload: {} }),
  });
  assert.equal(unauthorized.status, 401);

  const accepted = await fetch(`${baseUrl}/internal/publish`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-realtime-secret': 'internal-test-secret',
    },
    body: JSON.stringify({ type: 'task.created', eventId: 'http-1', payload: {} }),
  });
  assert.equal(accepted.status, 202);
});

test('task subscriptions fail closed unless Redis grants the authenticated user', async (t) => {
  const redis = new FakeRedis();
  const { baseUrl, secret } = await fixture(t, redis);
  const buyerId = '11111111-1111-4111-8111-111111111111';
  const taskId = '22222222-2222-4222-8222-222222222222';
  const client = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'BUYER', buyerId) },
  });
  t.after(() => client.close());
  await once(client, 'ready');

  const denied = once(client, 'task.subscription_denied');
  client.emit('task.subscribe', { taskId, helperId: 'attacker-controlled' });
  const [payload] = await denied;
  assert.equal(payload.taskId, taskId);
});

test('an authorized buyer receives the assigned helper snapshot after a gateway restart', async (t) => {
  const redis = new FakeRedis();
  const buyerId = '11111111-1111-4111-8111-111111111111';
  const helperId = '33333333-3333-4333-8333-333333333333';
  const taskId = '22222222-2222-4222-8222-222222222222';
  await redis.sadd(`him:rt:task:${taskId}:access`, buyerId, helperId);
  await redis.hset(`him:rt:task:${taskId}:assignment`, { taskId, buyerId, helperId });
  await redis.hset(`him:helper:${helperId}:state`, {
    lat: '17.385', lng: '78.4867', lastSeenEpochMs: '123456789', online: '1',
  });
  const { baseUrl, secret } = await fixture(t, redis);
  const client = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'BUYER', buyerId) },
  });
  t.after(() => client.close());
  await once(client, 'ready');

  const snapshot = once(client, 'helper.location');
  client.emit('task.subscribe', { taskId, helperId: 'ignored-untrusted-value' });
  const [payload] = await snapshot;
  assert.equal(payload.helperId, helperId);
  assert.equal(payload.lat, 17.385);
});

test('helper socket heartbeats refresh state TTL and the authoritative GEO index', async (t) => {
  const redis = new FakeRedis();
  const helperId = '33333333-3333-4333-8333-333333333333';
  await redis.hset(`him:helper:${helperId}:state`, { online: '1', h3: '' });
  const { baseUrl, secret } = await fixture(t, redis);
  const client = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'HELPER', helperId) },
  });
  t.after(() => client.close());
  await once(client, 'ready');

  const updated = once(client, 'location.updated');
  client.emit('location.update', { lat: 17.385, lng: 78.4867 });
  await updated;

  assert.ok(redis.calls.some((call) => call[0] === 'pexpire'
    && call[1] === `him:helper:${helperId}:state`));
  assert.ok(redis.calls.some((call) => call[0] === 'geoadd'
    && call[1] === 'him:online:helpers:geo'
    && call[4] === helperId));
});

test('terminal task status stops location forwarding even when helperId is omitted', async (t) => {
  const redis = new FakeRedis();
  const buyerId = '11111111-1111-4111-8111-111111111111';
  const helperId = '33333333-3333-4333-8333-333333333333';
  const taskId = '22222222-2222-4222-8222-222222222222';
  await redis.hset(`him:helper:${helperId}:state`, { online: '1', h3: '' });
  const { realtime, baseUrl, secret } = await fixture(t, redis);

  const buyer = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'BUYER', buyerId) },
  });
  const helper = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'HELPER', helperId) },
  });
  t.after(() => { buyer.close(); helper.close(); });
  await Promise.all([once(buyer, 'ready'), once(helper, 'ready')]);

  realtime.emitEvent({
    eventId: 'assignment-event',
    type: 'TASK_ASSIGNED',
    payload: { taskId, buyerId, helperId, status: 'ASSIGNED' },
  }, 'test');
  const firstLocation = once(buyer, 'helper.location');
  helper.emit('location.update', { lat: 17.385, lng: 78.4867 });
  await firstLocation;

  realtime.emitEvent({
    eventId: 'completed-event',
    type: 'TASK_STATUS_CHANGED',
    payload: { taskId, buyerId, status: 'COMPLETED' },
  }, 'test');

  let leaked = false;
  buyer.once('helper.location', () => { leaked = true; });
  const helperAfterCompletion = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'HELPER', helperId) },
  });
  t.after(() => helperAfterCompletion.close());
  await once(helperAfterCompletion, 'ready');
  const heartbeatAccepted = once(helperAfterCompletion, 'location.updated');
  helperAfterCompletion.emit('location.update', { lat: 17.386, lng: 78.487 });
  await heartbeatAccepted;
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(leaked, false);
});

test('task creation does not trigger a fleet-wide helper refresh', async (t) => {
  const { realtime, baseUrl, secret } = await fixture(t);
  const helperId = '33333333-3333-4333-8333-333333333333';
  const helper = createClient(baseUrl, {
    transports: ['websocket'], reconnection: false,
    auth: { token: accessToken(secret, 'HELPER', helperId) },
  });
  t.after(() => helper.close());
  await once(helper, 'ready');

  let globallyWoken = false;
  helper.once('task_created', () => { globallyWoken = true; });
  realtime.emitEvent({
    eventId: 'task-created-event',
    type: 'TASK_CREATED',
    payload: {
      taskId: '22222222-2222-4222-8222-222222222222',
      buyerId: '11111111-1111-4111-8111-111111111111',
    },
  }, 'test');
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(globallyWoken, false);
});
