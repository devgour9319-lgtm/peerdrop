const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');
const crypto   = require('crypto');
const os       = require('os');

const app    = express();
const server = http.createServer(app);

// ── Grace period: how long to keep a peer's slot after socket drops ──
const GRACE_MS = 15_000;  // 15 seconds

const io = new Server(server, {
  cors:              { origin: '*' },
  maxHttpBufferSize: 50 * 1024 * 1024,
  transports:        ['websocket', 'polling'],
  allowEIO3:         true,
  pingInterval:      8_000,    // heartbeat every 8 s
  pingTimeout:       25_000,   // dead after 25 s silence
});

app.use(express.static(path.join(__dirname, 'public')));

app.get('/local-info', (_, res) => {
  res.json({ ip: getLocalIP(), port: PORT, timestamp: Date.now() });
});

app.get('*', (_, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── Local IP ─────────────────────────────────────────────────────────────────
function getLocalIP() {
  const nets = os.networkInterfaces();
  const cands = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        if (/^(192\.168\.|10\.|172\.(1[6-9]|2\d|3[01])\.)/.test(net.address))
          cands.push({ addr: net.address, name });
      }
    }
  }
  const wifi = cands.find(c => /wi.fi|wlan|wl/i.test(c.name));
  return (wifi || cands[0])?.addr || null;
}

// ─── Room store ───────────────────────────────────────────────────────────────
const rooms = new Map();

function genId()    { return crypto.randomBytes(3).toString('hex').toUpperCase(); }
function genToken() { return crypto.randomBytes(16).toString('hex'); }
function stamp()    { return new Date().toLocaleTimeString(); }

// ─── Socket logic ─────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[${stamp()}] + ${socket.id.slice(0, 8)}`);

  // ── Create room ─────────────────────────────────────────────────────────────
  socket.on('create-room', () => {
    _silentLeave(socket);
    let id;
    do { id = genId(); } while (rooms.has(id));
    const token = genToken();

    rooms.set(id, {
      id, token,
      host:     socket.id,
      peers:    [socket.id],
      created:  Date.now(),
      graceTimers: {},          // socketId → setTimeout handle
    });

    socket.join(id);
    socket.currentRoom = id;
    socket.emit('room-created', { roomId: id, token, localIP: getLocalIP(), port: PORT });
    console.log(`[${stamp()}] Room ${id} created by ${socket.id.slice(0, 8)}`);
  });

  // ── Join room ───────────────────────────────────────────────────────────────
  socket.on('join-room', ({ roomId, token }) => {
    const cleanId = (roomId || '').toUpperCase().trim();
    const room = rooms.get(cleanId);
    if (!room) return socket.emit('join-error', { message: 'Room not found.' });

    // If this peer is rejoining within grace period, cancel the grace timer
    if (room.graceTimers[socket.id]) {
      clearTimeout(room.graceTimers[socket.id]);
      delete room.graceTimers[socket.id];
      console.log(`[${stamp()}] ${socket.id.slice(0, 8)} rejoined within grace period`);
    }

    // Check if room is full (but allow rejoining same socket)
    if (room.peers.length >= 2 && !room.peers.includes(socket.id)) {
      return socket.emit('join-error', { message: 'Room is full (1-to-1 only).' });
    }

    if (room.token && token && room.token !== token) {
      return socket.emit('join-error', { message: 'Invalid pairing token.' });
    }

    _silentLeave(socket);
    if (!room.peers.includes(socket.id)) {
      room.peers.push(socket.id);
    }
    socket.join(room.id);
    socket.currentRoom = room.id;

    socket.emit('room-joined', { roomId: room.id, hostId: room.host });
    socket.to(room.id).emit('peer-joined', { peerId: socket.id });
    console.log(`[${stamp()}] ${socket.id.slice(0, 8)} joined room ${room.id}`);
  });

  // ── WebRTC signaling relay ──────────────────────────────────────────────────
  socket.on('offer',         ({ offer,     to }) => io.to(to).emit('offer',         { offer,     from: socket.id }));
  socket.on('answer',        ({ answer,    to }) => io.to(to).emit('answer',        { answer,    from: socket.id }));
  socket.on('ice-candidate', ({ candidate, to }) => io.to(to).emit('ice-candidate', { candidate, from: socket.id }));

  // ── Explicit leave ──────────────────────────────────────────────────────────
  socket.on('leave-room', () => {
    _leaveAndNotify(socket, 'leave-room');
  });

  // ── Socket disconnect — start GRACE PERIOD instead of instant removal ──────
  socket.on('disconnect', (reason) => {
    console.log(`[${stamp()}] - ${socket.id.slice(0, 8)} (${reason})`);
    const rid = socket.currentRoom;
    if (!rid) return;
    const room = rooms.get(rid);
    if (!room) return;

    // Don't remove immediately — give them GRACE_MS to reconnect
    console.log(`[${stamp()}] Room ${rid}: starting ${GRACE_MS/1000}s grace for ${socket.id.slice(0, 8)}`);
    socket.to(rid).emit('peer-temporarily-gone', { peerId: socket.id, graceMs: GRACE_MS });

    room.graceTimers[socket.id] = setTimeout(() => {
      delete room.graceTimers[socket.id];
      // Grace expired — actually remove them
      room.peers = room.peers.filter(p => p !== socket.id);
      if (room.peers.length === 0) {
        rooms.delete(rid);
        console.log(`[${stamp()}] Room ${rid} closed (grace expired, empty)`);
      } else {
        io.to(rid).emit('peer-disconnected', { reason: `grace-expired(${reason})` });
        console.log(`[${stamp()}] Room ${rid}: peer-disconnected (grace expired)`);
      }
    }, GRACE_MS);

    socket.leave(rid);
    socket.currentRoom = null;
  });
});

// ─── _silentLeave ─────────────────────────────────────────────────────────────
function _silentLeave(socket) {
  const rid = socket.currentRoom;
  if (!rid) return;
  const room = rooms.get(rid);
  if (room) {
    // Cancel any grace timer for this socket
    if (room.graceTimers[socket.id]) {
      clearTimeout(room.graceTimers[socket.id]);
      delete room.graceTimers[socket.id];
    }
    room.peers = room.peers.filter(p => p !== socket.id);
    if (room.peers.length === 0 && Object.keys(room.graceTimers).length === 0) {
      rooms.delete(rid);
      console.log(`[${stamp()}] Room ${rid} closed (silent-leave, empty)`);
    }
  }
  socket.leave(rid);
  socket.currentRoom = null;
}

// ─── _leaveAndNotify ──────────────────────────────────────────────────────────
function _leaveAndNotify(socket, reason) {
  const rid = socket.currentRoom;
  if (!rid) return;
  const room = rooms.get(rid);
  if (room) {
    if (room.graceTimers[socket.id]) {
      clearTimeout(room.graceTimers[socket.id]);
      delete room.graceTimers[socket.id];
    }
    room.peers = room.peers.filter(p => p !== socket.id);
    if (room.peers.length === 0 && Object.keys(room.graceTimers).length === 0) {
      rooms.delete(rid);
      console.log(`[${stamp()}] Room ${rid} closed (${reason})`);
    } else {
      socket.to(rid).emit('peer-disconnected', { reason });
      console.log(`[${stamp()}] Room ${rid}: peer-disconnected (${reason})`);
    }
  }
  socket.leave(rid);
  socket.currentRoom = null;
}

// ─── Expire rooms older than 2 hours ─────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    if (now - room.created > 2 * 60 * 60_000) {
      // Cancel all grace timers
      for (const t of Object.values(room.graceTimers)) clearTimeout(t);
      rooms.delete(id);
      console.log(`[${stamp()}] Room ${id} expired (2h)`);
    }
  }
}, 10 * 60_000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log(`\n======================================================`);
  console.log(`⚡ PeerDrop v12 — Resilient Connection Server`);
  console.log(`   Grace period: ${GRACE_MS/1000}s | Keepalive: 8s/25s`);
  console.log(`📶 WiFi → http://${ip || 'localhost'}:${PORT}`);
  console.log(`💻 Local → http://localhost:${PORT}`);
  console.log(`======================================================\n`);
});
