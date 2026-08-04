const http = require('http');
const crypto = require('crypto');

const express = require('express');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const jwt = require('jsonwebtoken');
const { latLngToCell } = require('h3-js');

function requiredEnv(env, name) {
  const v = env[name];
  if (!v) {
    throw new Error(`Missing env var ${name}`);
  }
  return v;
}

function deriveHmacKey(secret) {
  return crypto.createHash('sha256').update(secret, 'utf8').digest();
}

function createRealtimeServer(options = {}) {
const env = options.env || process.env;
const PORT = Number(env.PORT || 8090);
const HOST = env.HOST || '127.0.0.1';
const REDIS_URL = env.REDIS_URL || 'redis://localhost:6379';
const JWT_ACCESS_SECRET = requiredEnv(env, 'JWT_ACCESS_SECRET');
const JWT_KEY = deriveHmacKey(JWT_ACCESS_SECRET);
const H3_RESOLUTION = Number(env.MATCH_H3_RESOLUTION || 9);
const REDIS_CHANNEL = env.REALTIME_REDIS_CHANNEL || 'him:rt:events';
const INTERNAL_SECRET = env.REALTIME_INTERNAL_SECRET || '';

const app = express();
app.use(express.json({ limit: '256kb' }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

const redis = options.redis || new Redis(REDIS_URL);
const sub = options.sub || new Redis(REDIS_URL);
const helperAssignments = new Map();
const recentEventIds = new Map();
let lastEventAt = null;
let lastEventType = null;
let redisSubHealthy = false;

function trimRecentEvents() {
  const now = Date.now();
  for (const [id, expiresAt] of recentEventIds.entries()) {
    if (expiresAt <= now) {
      recentEventIds.delete(id);
    }
  }
}

const recentEventsTimer = setInterval(trimRecentEvents, 30_000);
recentEventsTimer.unref();

function markEventSeen(eventId) {
  if (!eventId) return false;
  const now = Date.now();
  const expiresAt = recentEventIds.get(eventId);
  if (expiresAt && expiresAt > now) return true;
  recentEventIds.set(eventId, now + 2 * 60 * 1000);
  return false;
}

function emitEvent(evt, source) {
  if (!evt || typeof evt !== 'object') return;

  const eventId = evt.eventId || null;
  if (eventId && markEventSeen(eventId)) {
    return;
  }

  const rawType = evt.type || '';
  const type = rawType.toUpperCase().replace(/\./g, '_');
  const payload = evt.payload || {};
  lastEventAt = Date.now();
  lastEventType = type || 'UNKNOWN';

  if (type === 'CHAT_MESSAGE_RECEIVED') {
    if (payload.targetUserId) {
      io.to(`user:${payload.targetUserId}`).emit('chat_message_received', payload);
      io.to(`user:${payload.targetUserId}`).emit('chat.message.received', payload);
    }
    if (payload.taskId) {
      io.to(`task:${payload.taskId}`).emit('chat_message_received', payload);
      io.to(`task:${payload.taskId}`).emit('chat.message.received', payload);
    }
    return;
  }

  if (type === 'MEDIATOR_JOB_AVAILABLE') {
    for (const [, socket] of io.sockets.sockets) {
      if (socket.data.role === 'MEDIATOR' || socket.data.role === 'ADMIN') {
        socket.emit('mediator.job_available', payload);
      }
    }
    return;
  }

  if (type === 'KYC_REQUEST_SUBMITTED') {
    io.to('admin').emit('kyc.request_submitted', payload);
    io.to('admin').emit('admin.action_required', { type: 'KYC_REQUEST_SUBMITTED', ...payload });
    return;
  }

  if (type === 'MEDIATOR_JOB_ACCEPTED') {
    if (payload.buyerId) {
      io.to(`user:${payload.buyerId}`).emit('mediator.job_accepted', payload);
    }
    return;
  }

  if (type === 'MEDIATOR_JOB_DISPATCHED') {
    if (payload.buyerId) {
      io.to(`user:${payload.buyerId}`).emit('mediator.job_dispatched', payload);
    }
    if (payload.mediatorId) {
      io.to(`user:${payload.mediatorId}`).emit('mediator.job_dispatched', payload);
    }
    return;
  }

  if (type === 'MEDIATOR_ATTENDANCE_UPDATE') {
    if (payload.mediatorId) {
      io.to(`user:${payload.mediatorId}`).emit('mediator.attendance_update', payload);
    }
    return;
  }

  if (type === 'MEDIATOR_JOB_COMPLETED') {
    if (payload.buyerId) {
      io.to(`user:${payload.buyerId}`).emit('mediator.job_completed', payload);
    }
    if (payload.mediatorId) {
      io.to(`user:${payload.mediatorId}`).emit('mediator.job_completed', payload);
    }
    return;
  }

  if (type === 'TASK_OFFERED') {
    if (payload.helperId) {
      io.to(`user:${payload.helperId}`).emit('task.offered', payload);
    }
    return;
  }

  if (type === 'TASK_CREATED') {
    // Broadcast to all connected helpers so they can refresh available tasks
    for (const [, socket] of io.sockets.sockets) {
      if (socket.data.role === 'HELPER') {
        socket.emit('task_created', payload);
      }
    }
    // buyers don't need the "task_created" socket event, it only clutters
    // their client and was blamed for earlier notification bugs.
    return;
  }

  if (type === 'TASK_ASSIGNED' || type === 'TASK_STATUS_CHANGED') {
    const dotType = type.toLowerCase().replace(/_/g, '.');
    const snakeType = type.toLowerCase();
    const emitToTargets = (room) => {
      io.to(room).emit(dotType, payload);
      io.to(room).emit(snakeType, payload);
    };
    if (payload.buyerId) {
      emitToTargets(`user:${payload.buyerId}`);
    }
    if (payload.helperId) {
      emitToTargets(`user:${payload.helperId}`);
    }
    if (payload.taskId) {
      emitToTargets(`task:${payload.taskId}`);
    }
    if (type === 'TASK_ASSIGNED' && payload.helperId) {
      helperAssignments.set(payload.helperId, { taskId: payload.taskId, buyerId: payload.buyerId });
    }
    if (type === 'TASK_STATUS_CHANGED' && payload.helperId && payload.status) {
      if (payload.status === 'COMPLETED' || payload.status === 'CANCELLED') {
        helperAssignments.delete(payload.helperId);
      }
    }
    return;
  }

  // default: broadcast to task room when available
  if (payload.taskId) {
    io.to(`task:${payload.taskId}`).emit(type.toLowerCase(), payload);
  }
}

app.get('/health', (_req, res) =>
  res.json({
    ok: true,
    redisSubHealthy,
    sockets: io.engine.clientsCount,
    lastEventType,
    lastEventAt,
  }),
);

app.post('/internal/publish', (req, res) => {
  if (!INTERNAL_SECRET) {
    return res.status(503).json({ ok: false, code: 'REALTIME_INTERNAL_SECRET_NOT_SET' });
  }
  const secret = req.get('x-realtime-secret');
  if (!secret || secret !== INTERNAL_SECRET) {
    return res.status(401).json({ ok: false, code: 'UNAUTHORIZED' });
  }
  try {
    emitEvent(req.body, 'http');
    return res.status(202).json({ ok: true });
  } catch (err) {
    console.error('Failed to process /internal/publish event', err);
    return res.status(400).json({ ok: false, code: 'INVALID_EVENT' });
  }
});

io.use((socket, next) => {
  try {
    const token =
      (socket.handshake.auth && socket.handshake.auth.token) ||
      (socket.handshake.headers.authorization || '').replace(/^Bearer\s+/i, '').trim();

    if (!token) {
      return next(new Error('AUTH_REQUIRED'));
    }

    const decoded = jwt.verify(token, JWT_KEY, { algorithms: ['HS256'] });
    if (!decoded || decoded.type !== 'access' || !decoded.sub || !decoded.role) {
      return next(new Error('INVALID_TOKEN'));
    }

    socket.data.userId = decoded.sub;
    socket.data.role = decoded.role;
    return next();
  } catch (e) {
    return next(new Error('INVALID_TOKEN'));
  }
});

io.on('connection', (socket) => {
  const userId = socket.data.userId;
  const role = socket.data.role;

  socket.join(`user:${userId}`);
  if (role === 'ADMIN' || role === 'KYC' || role === 'SUPPORT') {
    socket.join('admin');
  }
  socket.emit('ready', { userId, role });

  socket.on('task.subscribe', async (msg) => {
    if (!msg || !msg.taskId) return;
    socket.join(`task:${msg.taskId}`);
    if (msg.helperId) {
      try {
        const stateKey = `him:helper:${msg.helperId}:state`;
        const [latStr, lngStr, tsStr] = await redis.hmget(stateKey, 'lat', 'lng', 'lastSeenEpochMs');
        const lat = Number(latStr);
        const lng = Number(lngStr);
        if (Number.isFinite(lat) && Number.isFinite(lng)) {
          socket.emit('helper.location', {
            taskId: msg.taskId,
            helperId: msg.helperId,
            lat,
            lng,
            ts: tsStr ? Number(tsStr) : Date.now(),
          });
        }
      } catch (err) {
        // ignore Redis error
      }
    }
  });

  socket.on('location.update', async (msg) => {
    try {
      if (role !== 'HELPER') return;
      const lat = Number(msg && msg.lat);
      const lng = Number(msg && msg.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

      // Do not allow socket location updates to bring helpers "online" (bypass KYC).
      // Going online must happen via the API which enforces helper approval.
      const stateKey = `him:helper:${userId}:state`;
      const isOnline = await redis.hget(stateKey, 'online');
      if (isOnline !== '1') return;

      // Java backend stores H3 indexes as unsigned decimal strings (Long.toUnsignedString).
      // h3-js returns a hex string; convert to unsigned decimal so both components share keys.
      const cellHex = latLngToCell(lat, lng, H3_RESOLUTION);
      const cell = BigInt('0x' + cellHex).toString();

      const prevCell = await redis.hget(stateKey, 'h3');
      if (prevCell && prevCell !== cell) {
        await redis.srem(`him:online:h3:${prevCell}`, userId);
      }

      const nowMs = Date.now();
      await redis.hset(stateKey, {
        lat: String(lat),
        lng: String(lng),
        h3: cell,
        lastSeenEpochMs: String(nowMs),
      });
      await redis.sadd(`him:online:h3:${cell}`, userId);

      io.to(`user:${userId}`).emit('location.updated', { lat, lng, cell, cellHex, ts: nowMs });

      const assignment = helperAssignments.get(userId);
      if (assignment && assignment.taskId) {
        const payload = {
          taskId: assignment.taskId,
          helperId: userId,
          lat,
          lng,
          ts: nowMs,
        };
        io.to(`task:${assignment.taskId}`).emit('helper.location', payload);
        if (assignment.buyerId) {
          io.to(`user:${assignment.buyerId}`).emit('helper.location', payload);
        }
      }
    } catch (_e) {
      // best-effort
    }
  });
});

sub.subscribe(REDIS_CHANNEL, (err) => {
  if (err) {
    redisSubHealthy = false;
    console.error('Failed to subscribe to', REDIS_CHANNEL, err);
    return;
  }
  redisSubHealthy = true;
  console.log('Subscribed to', REDIS_CHANNEL);
});

sub.on('error', (err) => {
  redisSubHealthy = false;
  console.error('Redis subscriber error', err);
});

redis.on('error', (err) => {
  console.error('Redis client error', err);
});

sub.on('message', (_channel, message) => {
  try {
    const evt = JSON.parse(message);
    emitEvent(evt, 'redis');
  } catch (_e) {
    // ignore malformed
  }
});

function start() {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(PORT, HOST, () => {
      server.off('error', reject);
      console.log(`helpinminutes-realtime listening on ${HOST}:${PORT}`);
      resolve(server.address());
    });
  });
}

async function stop() {
  clearInterval(recentEventsTimer);
  await new Promise((resolve) => io.close(resolve));
  await Promise.allSettled([
    typeof redis.quit === 'function' ? redis.quit() : Promise.resolve(),
    typeof sub.quit === 'function' ? sub.quit() : Promise.resolve(),
  ]);
}

return { app, server, io, emitEvent, start, stop };
}

if (require.main === module) {
  createRealtimeServer().start().catch((err) => {
    console.error('Failed to start realtime server', err);
    process.exitCode = 1;
  });
}

module.exports = { createRealtimeServer, deriveHmacKey };
