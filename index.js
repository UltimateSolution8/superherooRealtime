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
  // Only `cors` used to be set here, leaving everything else at library
  // defaults. nginx proxy_read_timeout is 75s, comfortably above ping
  // interval + timeout, so a healthy connection is never cut mid-heartbeat.
  pingInterval: 25_000,
  pingTimeout: 20_000,
  connectTimeout: 20_000,
  // Nothing this gateway accepts is large; the cap bounds what a single client
  // can make the process allocate.
  maxHttpBufferSize: 1e5,
  // CPU is the scarce resource on this box, not bandwidth, and the payloads are
  // small JSON objects that compress poorly per-message.
  perMessageDeflate: false,
});

// ioredis defaults leave commands queued indefinitely against an unreachable
// server and retry forever. These bound both.
const redisOptions = {
  connectTimeout: 5_000,
  keepAlive: 30_000,
  enableReadyCheck: true,
  maxRetriesPerRequest: 3,
  retryStrategy: (times) => Math.min(times * 200, 2_000),
};

const redis = options.redis || new Redis(REDIS_URL, redisOptions);
// The subscriber must never give up reconnecting — losing it means the gateway
// silently stops delivering every backend event. Offline queueing is disabled
// because a queued SUBSCRIBE replayed after reconnect is redundant: ioredis
// re-subscribes on its own.
const sub = options.sub || new Redis(REDIS_URL, {
  ...redisOptions,
  maxRetriesPerRequest: null,
  enableOfflineQueue: false,
});

// helperId -> { taskId, buyerId, expiresAt }. Entries are removed when a task
// reaches COMPLETED or CANCELLED, but a crash, a missed event or a task stuck in
// any other terminal state used to leave one behind forever. expiresAt is swept
// by the existing recentEvents timer.
const helperAssignments = new Map();
const ASSIGNMENT_TTL_MS = 6 * 60 * 60 * 1000;
// Minimum gap between accepted location updates from one socket. Nothing
// rate-limited this before, so a buggy or hostile client could drive unbounded
// Redis writes.
const LOCATION_UPDATE_MIN_INTERVAL_MS = 2_000;
// How long a socket may reuse its cached online flag before re-reading it.
// Trades a few seconds of staleness for one fewer billable Redis command on the
// hottest path; the API remains the authority on going online.
const ONLINE_STATE_CACHE_MS = 30_000;
// TTL on the per-cell membership sets. They are only a cold-start fallback for the
// backend's GEO index, so expiry is harmless and stops unbounded key growth.
const H3_SET_TTL_MS = 30 * 60 * 1000;
const HELPER_STATE_TTL_MS = 10 * 60 * 1000;
const TASK_ACCESS_TTL_SECONDS = 7 * 24 * 60 * 60;
const ASSIGNMENT_REDIS_TTL_SECONDS = 6 * 60 * 60;
const ONLINE_HELPERS_GEO_KEY = 'him:online:helpers:geo';
// Ceiling on rooms a single socket may join (its own id, user:, role:, admin,
// plus task: subscriptions). Generous for real use; bounds abuse.
const MAX_ROOMS_PER_SOCKET = 60;
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
  for (const [helperId, assignment] of helperAssignments.entries()) {
    if (assignment.expiresAt <= now) {
      helperAssignments.delete(helperId);
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

function isUuid(value) {
  return typeof value === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function taskAccessKey(taskId) {
  return `him:rt:task:${taskId}:access`;
}

function taskAssignmentKey(taskId) {
  return `him:rt:task:${taskId}:assignment`;
}

/**
 * Persists the minimum routing state needed after a gateway restart.
 *
 * Pub/Sub is intentionally ephemeral. Without this, restarting Node forgot every
 * active assignment: buyers stopped receiving live location until another task
 * status event happened, and task.subscribe had no trustworthy authorization
 * source at all.
 */
function persistTaskRoutingState(type, payload) {
  const taskId = payload && payload.taskId;
  if (!isUuid(taskId)) return;
  try {
    const pipeline = redis.pipeline();
    const accessKey = taskAccessKey(taskId);
    if (payload.buyerId) pipeline.sadd(accessKey, String(payload.buyerId));
    if (payload.helperId) pipeline.sadd(accessKey, String(payload.helperId));
    pipeline.expire(accessKey, TASK_ACCESS_TTL_SECONDS);

    if (type === 'TASK_ASSIGNED' && payload.helperId) {
      const assignmentKey = taskAssignmentKey(taskId);
      pipeline.hset(assignmentKey, {
        taskId,
        helperId: String(payload.helperId),
        buyerId: payload.buyerId ? String(payload.buyerId) : '',
      });
      pipeline.expire(assignmentKey, ASSIGNMENT_REDIS_TTL_SECONDS);
    }
    if (type === 'TASK_STATUS_CHANGED'
        && ['COMPLETED', 'CANCELLED'].includes(String(payload.status || '').toUpperCase())) {
      pipeline.del(taskAssignmentKey(taskId));
      pipeline.expire(accessKey, 60 * 60);
    }
    pipeline.exec().catch((err) => console.error('Failed to persist task routing state', err));
  } catch (err) {
    console.error('Failed to prepare task routing state', err);
  }
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
  if (type === 'TASK_CREATED' || type === 'TASK_ASSIGNED' || type === 'TASK_STATUS_CHANGED') {
    persistTaskRoutingState(type, payload);
  }

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
    // Role rooms, not a scan of every connected socket. The old form was O(all
    // sockets) per event and, being process-local, would also have been wrong
    // the moment a second instance existed.
    io.to('role:MEDIATOR').emit('mediator.job_available', payload);
    io.to('role:ADMIN').emit('mediator.job_available', payload);
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
    // Do not wake every partner for every task. At 5,000 online partners that
    // turned one booking into 5,000 pull-feed requests. Matching emits targeted
    // TASK_OFFERED events immediately; the jittered pull feed is the recovery path.
    // TASK_CREATED is retained in the stream to persist buyer task-room access.
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
      helperAssignments.set(payload.helperId, {
        taskId: payload.taskId,
        buyerId: payload.buyerId,
        expiresAt: Date.now() + ASSIGNMENT_TTL_MS,
      });
    }
    if (type === 'TASK_STATUS_CHANGED' && payload.status
        && ['COMPLETED', 'CANCELLED'].includes(String(payload.status).toUpperCase())) {
      if (payload.helperId) {
        helperAssignments.delete(payload.helperId);
      } else if (payload.taskId) {
        // Most status events identify the task and buyer but omit helperId. Never
        // keep forwarding a former partner's location merely because that optional
        // field was absent from the terminal event.
        for (const [helperId, assignment] of helperAssignments.entries()) {
          if (assignment.taskId === payload.taskId) helperAssignments.delete(helperId);
        }
      }
    }
    return;
  }

  // default: broadcast to task room when available
  if (payload.taskId) {
    io.to(`task:${payload.taskId}`).emit(type.toLowerCase(), payload);
  }
}

app.get('/health', (_req, res) => {
  // 503 when the subscriber is down. This used to return 200 unconditionally,
  // so the nginx /realtime/health probe reported healthy on a gateway that was
  // delivering no backend events at all.
  const ok = redisSubHealthy;
  res.status(ok ? 200 : 503).json({
    ok,
    redisSubHealthy,
    sockets: io.engine.clientsCount,
    lastEventType,
    lastEventAt,
  });
});

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
  // Role room, so legitimate role-wide events are a room emit rather than a scan
  // over every connected socket. Task offers remain individually targeted.
  if (role) {
    socket.join(`role:${role}`);
  }
  if (role === 'ADMIN' || role === 'KYC' || role === 'SUPPORT') {
    socket.join('admin');
  }
  socket.emit('ready', { userId, role });

  socket.on('task.subscribe', async (msg) => {
    if (!msg || !isUuid(msg.taskId)) return;
    // Cap how many task rooms one socket can accumulate. Nothing bounded this,
    // so a client in a loop could grow the adapter's room maps without limit.
    if (socket.rooms.size >= MAX_ROOMS_PER_SOCKET) return;
    try {
      const taskId = msg.taskId;
      const privileged = role === 'ADMIN' || role === 'SUPPORT';
      const granted = privileged || Boolean(await redis.sismember(taskAccessKey(taskId), userId));
      if (!granted) {
        socket.emit('task.subscription_denied', { taskId });
        return;
      }

      socket.join(`task:${taskId}`);
      const assignment = await redis.hgetall(taskAssignmentKey(taskId));
      const helperId = assignment && assignment.helperId;
      if (helperId && (privileged || userId === helperId || userId === assignment.buyerId)) {
        helperAssignments.set(helperId, {
          taskId,
          buyerId: assignment.buyerId || null,
          expiresAt: Date.now() + ASSIGNMENT_TTL_MS,
        });
        const stateKey = `him:helper:${helperId}:state`;
        const [latStr, lngStr, tsStr] = await redis.hmget(stateKey, 'lat', 'lng', 'lastSeenEpochMs');
        const lat = Number(latStr);
        const lng = Number(lngStr);
        if (Number.isFinite(lat) && Number.isFinite(lng)) {
          socket.emit('helper.location', {
            taskId,
            helperId,
            lat,
            lng,
            ts: tsStr ? Number(tsStr) : Date.now(),
          });
        }
      }
    } catch (err) {
      // Authorization must fail closed. User-room events continue to work while
      // Redis recovers; joining an unverified task room does not.
      socket.emit('task.subscription_unavailable', { taskId: msg.taskId });
    }
  });

  socket.on('location.update', async (msg) => {
    try {
      if (role !== 'HELPER') return;
      const lat = Number(msg && msg.lat);
      const lng = Number(msg && msg.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)
          || lat < 6 || lat > 38 || lng < 68 || lng > 98) return;

      // Throttle per socket. Partners heartbeat roughly every 15s, so a 2s floor
      // never rejects a legitimate update but caps what a misbehaving client can
      // push into Redis.
      const nowMs = Date.now();
      if (socket.data.lastLocationAt
          && nowMs - socket.data.lastLocationAt < LOCATION_UPDATE_MIN_INTERVAL_MS) {
        return;
      }
      socket.data.lastLocationAt = nowMs;

      // Do not allow socket location updates to bring helpers "online" (bypass KYC).
      // Going online must happen via the API which enforces helper approval.
      const stateKey = `him:helper:${userId}:state`;

      // Java backend stores H3 indexes as unsigned decimal strings (Long.toUnsignedString).
      // h3-js returns a hex string; convert to unsigned decimal so both components share keys.
      const cellHex = latLngToCell(lat, lng, H3_RESOLUTION);
      const cell = BigInt('0x' + cellHex).toString();

      // Online state is re-read at most once every ONLINE_STATE_CACHE_MS per socket
      // rather than on every update. Redis is billed per command and this is the
      // gateway's highest-frequency path: at the 2s throttle floor the read alone was
      // 30 commands a minute per socket. The cached window is short enough that a
      // partner going offline stops broadcasting promptly, and the API — not this —
      // is the authority on whether they may be online at all.
      const cached = socket.data.onlineState;
      const cacheIsFresh = cached && nowMs - cached.at < ONLINE_STATE_CACHE_MS;
      let isOnline;
      let prevCell;
      if (cacheIsFresh) {
        isOnline = cached.online;
        prevCell = cached.cell;
      } else {
        [isOnline, prevCell] = await redis.hmget(stateKey, 'online', 'h3');
      }

      // `readAt` is when the online flag last came from Redis, not when it was last
      // used — otherwise every update would refresh the window and the flag would
      // never be re-read at all.
      const readAt = cacheIsFresh ? cached.at : nowMs;

      if (isOnline !== '1') {
        // Cache the negative too: an offline socket that keeps emitting should not
        // cost a read every couple of seconds.
        socket.data.onlineState = { at: readAt, online: isOnline, cell: prevCell };
        return;
      }
      socket.data.onlineState = { at: readAt, online: isOnline, cell };

      const pipeline = redis.pipeline();
      pipeline.hset(stateKey, {
        lat: String(lat),
        lng: String(lng),
        h3: cell,
        lastSeenEpochMs: String(nowMs),
      });
      pipeline.pexpire(stateKey, HELPER_STATE_TTL_MS);
      // This is the authoritative nearby-candidate index used by the backend.
      // The socket heartbeat must update it just like PUT /helpers/online does.
      pipeline.geoadd(ONLINE_HELPERS_GEO_KEY, lng, lat, userId);
      // The per-cell sets are only rewritten when the cell actually changes.
      // Re-adding the same member to the same set every couple of seconds was a
      // billable no-op, and a partner is stationary most of the time. The TTL keeps
      // these bounded: without one, every cell anyone ever passed through left a
      // permanent key behind.
      if (prevCell !== cell) {
        if (prevCell) {
          pipeline.srem(`him:online:h3:${prevCell}`, userId);
        }
        pipeline.sadd(`him:online:h3:${cell}`, userId);
        pipeline.pexpire(`him:online:h3:${cell}`, H3_SET_TTL_MS);
      }
      await pipeline.exec();

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

// ioredis re-subscribes automatically after a reconnect, but nothing used to
// clear the unhealthy flag — so one transient blip left /health reporting a
// permanently sick gateway that was in fact working.
sub.on('ready', () => {
  redisSubHealthy = true;
  console.log('Redis subscriber ready');
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
  const instance = createRealtimeServer();
  instance.start().catch((err) => {
    console.error('Failed to start realtime server', err);
    process.exitCode = 1;
  });

  // stop() existed but nothing ever called it, so systemd's SIGTERM killed the
  // process outright and every deploy cut live sockets. Clients reconnect, but
  // they lose the in-flight location stream and show the reconnecting banner.
  // The unit allows 20s (TimeoutStopSec) for this to finish.
  let shuttingDown = false;
  for (const signal of ['SIGTERM', 'SIGINT']) {
    process.once(signal, () => {
      if (shuttingDown) return;
      shuttingDown = true;
      console.log(`Received ${signal}, shutting down realtime gateway`);
      instance
        .stop()
        .catch((err) => console.error('Error during shutdown', err))
        .finally(() => process.exit(0));
    });
  }
}

module.exports = { createRealtimeServer, deriveHmacKey };
