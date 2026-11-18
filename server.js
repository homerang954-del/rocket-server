const { WebSocketServer } = require('ws');
const port = process.env.PORT || 8080;
const wss = new WebSocketServer({ port });
let queue = [];
let nextId = 1;

wss.on('connection', (ws) => {
  ws.id = nextId++;
  ws.peer = null;
  ws.send(JSON.stringify({ type: 'hello', id: ws.id }));

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'queue') {
      if (ws.peer || queue.includes(ws)) return;
      queue.push(ws);
      if (queue.length >= 2) {
        const a = queue.shift();
        const b = queue.shift();
        const room = 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
        a.peer = b; b.peer = a;
        a.send(JSON.stringify({ type: 'matched', room, host: true }));
        b.send(JSON.stringify({ type: 'matched', room, host: false }));
      }
    } else if (m.type === 'state' && ws.peer && ws.peer.readyState === 1) {
      ws.peer.send(JSON.stringify({ type: 'state', data: m.data }));
    }
  });

  ws.on('close', () => {
    queue = queue.filter((x) => x !== ws);
    if (ws.peer) {
      if (ws.peer.readyState === 1) ws.peer.send(JSON.stringify({ type: 'left' }));
      ws.peer.peer = null;
    }
  });
});

console.log('Rocket server listening on port', port);
