const http = require('http');
const crypto = require('crypto');

const express = require('express');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const jwt = require('jsonwebtoken');
const { latLngToCell } = require('h3-js');

function requiredEnv(name) {
  const v = process.env[name];
  if (!v) {
    throw new Error(`Missing env var ${name}`);
  }
  return v;
}

function deriveHmacKey(secret) {
  return crypto.createHash('sha256').update(secret, 'utf8').digest();
}

const PORT = Number(process.env.PORT || 8090);
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const JWT_ACCESS_SECRET = requiredEnv('JWT_ACCESS_SECRET');
const JWT_KEY = deriveHmacKey(JWT_ACCESS_SECRET);
const H3_RESOLUTION = Number(process.env.MATCH_H3_RESOLUTION || 9);
const REDIS_CHANNEL = process.env.REALTIME_REDIS_CHANNEL || 'him:rt:events';

const app = express();
app.get('/health', (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

const redis = new Redis(REDIS_URL);
const sub = new Redis(REDIS_URL);
const helperAssignments = new Map();

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
  socket.emit('ready', { userId, role });

  socket.on('task.subscribe', (msg) => {
    if (!msg || !msg.taskId) return;
    socket.join(`task:${msg.taskId}`);
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
    console.error('Failed to subscribe to', REDIS_CHANNEL, err);
    process.exit(1);
  }
  console.log('Subscribed to', REDIS_CHANNEL);
});

sub.on('message', (_channel, message) => {
  try {
    const evt = JSON.parse(message);
    const rawType = evt.type || '';
    const type = rawType.toUpperCase().replace(/\./g, '_');
    const payload = evt.payload || {};

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
      // Also notify the buyer who created the task
      if (payload.buyerId) {
        io.to(`user:${payload.buyerId}`).emit('task_created', payload);
      }
      return;
    }

    if (type === 'TASK_ASSIGNED' || type === 'TASK_STATUS_CHANGED') {
      const emitType = type.toLowerCase().replace(/_/g, '.');
      if (payload.buyerId) {
        io.to(`user:${payload.buyerId}`).emit(emitType, payload);
      }
      if (payload.helperId) {
        io.to(`user:${payload.helperId}`).emit(emitType, payload);
      }
      if (payload.taskId) {
        io.to(`task:${payload.taskId}`).emit(emitType, payload);
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
  } catch (_e) {
    // ignore malformed
  }
});

server.listen(PORT, () => {
  console.log(`helpinminutes-realtime listening on :${PORT}`);
});
