const assert = require('node:assert/strict');
const { EventEmitter, once } = require('node:events');
const test = require('node:test');

const jwt = require('jsonwebtoken');
const { io: createClient } = require('socket.io-client');
const { createRealtimeServer, deriveHmacKey } = require('../index');

class FakeRedis extends EventEmitter {
  subscribe(_channel, callback) { callback(null, 1); }
  quit() { return Promise.resolve(); }
  hmget() { return Promise.resolve([null, null, null]); }
  hget() { return Promise.resolve(null); }
  hset() { return Promise.resolve(); }
  sadd() { return Promise.resolve(); }
  srem() { return Promise.resolve(); }
}

function accessToken(secret, role = 'ADMIN', subject = 'admin-1') {
  return jwt.sign({ type: 'access', role }, deriveHmacKey(secret), {
    algorithm: 'HS256',
    subject,
    expiresIn: '5m',
  });
}

async function fixture(t) {
  const secret = 'a-test-access-secret-with-enough-entropy';
  const realtime = createRealtimeServer({
    env: {
      PORT: '0',
      HOST: '127.0.0.1',
      JWT_ACCESS_SECRET: secret,
      REALTIME_INTERNAL_SECRET: 'internal-test-secret',
    },
    redis: new FakeRedis(),
    sub: new FakeRedis(),
  });
  const address = await realtime.start();
  const baseUrl = `http://127.0.0.1:${address.port}`;
  t.after(() => realtime.stop());
  return { realtime, baseUrl, secret };
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
