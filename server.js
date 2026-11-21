'use strict';
// Сервер «Змейки-арены»: раздаёт страницу и пересылает данные между игроками по WebSocket.
// Без внешних библиотек — нужен только Node.js 18+.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const INDEX = fs.readFileSync(path.join(__dirname, 'index.html'));
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PER_ROOM = 40;
const MAX_TOTAL = 200;
const MAX_PAYLOAD = 16 * 1024;
const MAX_MSG = 4096;
const TOPICS = new Set(['ate', 'die']);

const rooms = new Map(); // имя комнаты -> Map(peerId -> клиент)
let total = 0;

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(INDEX);
  } else if (u === '/health' || u === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found');
  }
});

function frame(opcode, payload) {
  const len = payload.length;
  let h;
  if (len < 126) h = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | opcode; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | opcode; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}

function sendFrame(c, buf, droppable) {
  if (c.dead) return;
  // медленному клиенту не копим очередь
  if (droppable && c.socket.writableLength > 256 * 1024) return;
  try { c.socket.write(buf); } catch (_) { kill(c); }
}
function sendJSON(c, obj, droppable) {
  sendFrame(c, frame(1, Buffer.from(JSON.stringify(obj))), droppable);
}
function broadcast(room, obj, except, droppable) {
  const buf = frame(1, Buffer.from(JSON.stringify(obj)));
  for (const c of room.values()) if (c !== except) sendFrame(c, buf, droppable);
}

function kill(c) {
  if (c.dead) return;
  c.dead = true;
  try { c.socket.destroy(); } catch (_) {}
  const room = rooms.get(c.roomName);
  if (room) {
    room.delete(c.id);
    total--;
    if (room.size === 0) rooms.delete(c.roomName);
    else broadcast(room, { t: 'l', peer: c.id }, null, false);
  }
}

function handleMessage(c, text) {
  // ограничение частоты: ~80 сообщений/с, всплеск до 160
  const now = Date.now();
  c.tokens = Math.min(160, c.tokens + (now - c.lastTok) * 0.08);
  c.lastTok = now;
  if (c.tokens < 1) return;
  c.tokens -= 1;
  if (text.length > MAX_MSG) return;
  let m;
  try { m = JSON.parse(text); } catch (_) { return; }
  if (!m || typeof m !== 'object') return;
  const room = rooms.get(c.roomName);
  if (!room) return;
  if (m.t === 'p' && m.d && typeof m.d === 'object' && !Array.isArray(m.d)) {
    for (const k of Object.keys(m.d)) {
      if (m.d[k] === null) delete c.presence[k]; else c.presence[k] = m.d[k];
    }
    if (JSON.stringify(c.presence).length > MAX_MSG) { c.presence = {}; return; }
    broadcast(room, { t: 'p', peer: c.id, d: m.d }, c, true);
  } else if (m.t === 'e' && TOPICS.has(m.topic)) {
    broadcast(room, { t: 'e', topic: m.topic, peer: c.id, d: m.d }, c, false);
  }
}

function onData(c, chunk) {
  c.buf = c.buf.length ? Buffer.concat([c.buf, chunk]) : chunk;
  for (;;) {
    const buf = c.buf;
    if (buf.length < 2) return;
    const b0 = buf[0], b1 = buf[1];
    const fin = !!(b0 & 0x80), op = b0 & 15, masked = !!(b1 & 0x80);
    let len = b1 & 127, off = 2;
    if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_PAYLOAD || !masked) return kill(c);
    if (buf.length < off + 4 + len) return;
    const mask = buf.subarray(off, off + 4);
    const payload = Buffer.from(buf.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    c.buf = buf.subarray(off + 4 + len);
    c.alive = true;
    if (op === 8) { try { c.socket.write(frame(8, Buffer.alloc(0))); } catch (_) {} return kill(c); }
    if (op === 9) { sendFrame(c, frame(10, payload), false); continue; }
    if (op === 10) continue;
    if (op === 1 || op === 2 || op === 0) {
      if (op !== 0) c.fragOp = op;
      c.frag.push(payload);
      if (c.frag.length > 8) { c.frag = []; return kill(c); }
      if (fin) {
        const all = c.frag.length === 1 ? c.frag[0] : Buffer.concat(c.frag);
        c.frag = [];
        if (c.fragOp === 1) handleMessage(c, all.toString('utf8'));
      }
    }
  }
}

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  const key = req.headers['sec-websocket-key'];
  if (url.pathname !== '/ws' || !key || total >= MAX_TOTAL) {
    socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  let roomName = (url.searchParams.get('room') || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 24) || 'main';
  let room = rooms.get(roomName);
  if (room && room.size >= MAX_PER_ROOM) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  if (!room) { room = new Map(); rooms.set(roomName, room); }
  const c = {
    id: crypto.randomBytes(4).toString('hex'), socket, roomName, presence: {},
    buf: Buffer.alloc(0), frag: [], fragOp: 1, alive: true, dead: false, tokens: 160, lastTok: Date.now()
  };
  room.set(c.id, c); total++;
  socket.on('data', d => { try { onData(c, d); } catch (_) { kill(c); } });
  socket.on('close', () => kill(c));
  socket.on('error', () => kill(c));
  const peers = [];
  for (const o of room.values()) if (o !== c) peers.push({ peer: o.id, presence: o.presence });
  sendJSON(c, { t: 'hello', peer: c.id, peers }, false);
});

// пинги, чтобы хостинг не обрывал тихие соединения, и уборка мёртвых
setInterval(() => {
  for (const room of rooms.values()) {
    for (const c of [...room.values()]) {
      if (!c.alive) { kill(c); continue; }
      c.alive = false;
      sendFrame(c, frame(9, Buffer.alloc(0)), false);
    }
  }
}, 20000);

server.listen(PORT, () => console.log('Змейка-арена: порт ' + PORT));
