const { WebSocketServer } = require('ws');
const port = process.env.PORT || 8080;
const wss = new WebSocketServer({ port });
const queues = { Unranked: [], Ranked: [] };
const privates = new Map();
let nextId = 1;

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const newRoom = () => 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
const newCode = () => {
  let c;
  do { c = Math.random().toString(36).slice(2, 7).toUpperCase(); } while (privates.has(c));
  return c;
};
const MAPS = ['Daylight', 'Night', 'Neon'];
const pair = (a, b) => {
  a.peer = b; b.peer = a;
  const room = newRoom();
  const map = MAPS[Math.floor(Math.random() * MAPS.length)];
  send(a, { type: 'matched', room, host: true, map });
  send(b, { type: 'matched', room, host: false, map });
};

wss.on('connection', (ws) => {
  ws.id = nextId++;
  ws.peer = null;
  ws.mode = null;
  ws.code = null;
  send(ws, { type: 'hello', id: ws.id });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'queue') {
      if (ws.peer || ws.mode) return;
      const mode = queues[m.mode] ? m.mode : 'Unranked';
      ws.mode = mode;
      queues[mode].push(ws);
      if (queues[mode].length >= 2) {
        const a = queues[mode].shift();
        const b = queues[mode].shift();
        a.mode = null; b.mode = null;
        pair(a, b);
      }
    } else if (m.type === 'create') {
      if (ws.peer || ws.mode) return;
      const code = newCode();
      ws.code = code;
      ws.mode = 'Private';
      privates.set(code, ws);
      send(ws, { type: 'code', code });
    } else if (m.type === 'join') {
      if (ws.peer || ws.mode) return;
      const code = String(m.code || '').toUpperCase().trim();
      const host = privates.get(code);
      if (!host || host.readyState !== 1 || host === ws) {
        send(ws, { type: 'error', msg: 'No room with that code.' });
        return;
      }
      privates.delete(code);
      host.code = null; host.mode = null;
      ws.mode = null;
      pair(host, ws);
    } else if (m.type === 'skip' && ws.peer && ws.peer.readyState === 1) {
      send(ws.peer, { type: 'skip' });
    } else if (m.type === 'state' && ws.peer && ws.peer.readyState === 1) {
      send(ws.peer, { type: 'state', data: m.data });
    }
  });

  ws.on('close', () => {
    if (ws.mode && queues[ws.mode]) queues[ws.mode] = queues[ws.mode].filter((x) => x !== ws);
    if (ws.code) privates.delete(ws.code);
    if (ws.peer) {
      send(ws.peer, { type: 'left' });
      ws.peer.peer = null;
    }
  });
});

console.log('Rocket server listening on port', port);
