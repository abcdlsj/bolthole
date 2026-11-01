// Cross-language tests: the JS SDK against the Go relay, Go agent and Go
// client (built from this repository).
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WebSocketServer } from 'ws';
import { Client, Agent } from '../src/index.js';

const dir = mkdtempSync(join(tmpdir(), 'bolthole-'));
const bin = join(dir, 'bolthole');
const procs = [];
let relay, upstream, wss, goConn;

const port = () => 20000 + Math.floor(Math.random() * 20000);

function run(args, waitFor) {
  const p = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  procs.push(p);
  let out = '';
  return new Promise((resolve, reject) => {
    const onData = (d) => {
      out += d;
      const m = out.match(waitFor);
      if (m) resolve(m);
    };
    p.stdout.on('data', onData);
    p.stderr.on('data', onData);
    p.on('exit', (code) => reject(new Error(`${args[0]} exited ${code}: ${out}`)));
    setTimeout(() => reject(new Error(`timeout waiting for ${waitFor}: ${out}`)), 20000);
  });
}

before(async () => {
  execFileSync('go', ['build', '-o', bin, '../cmd/bolthole'], { stdio: 'inherit' });

  upstream = createServer((req, res) => {
    if (req.url.startsWith('/hello')) return res.end('hello from node upstream');
    if (req.url === '/echo') {
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => res.end(Buffer.concat(chunks)));
      return;
    }
    res.statusCode = 404;
    res.end();
  });
  wss = new WebSocketServer({ server: upstream });
  wss.on('connection', (ws) => ws.on('message', (m, isBinary) => ws.send(isBinary ? m : 'echo:' + m)));
  await new Promise((r) => upstream.listen(0, '127.0.0.1', r));

  // BOLTHOLE_RELAY runs the suite against another relay edition, e.g. the
  // Cloudflare Worker under `wrangler dev`.
  if (process.env.BOLTHOLE_RELAY) {
    relay = process.env.BOLTHOLE_RELAY;
  } else {
    const rp = port();
    relay = `http://127.0.0.1:${rp}`;
    await run(['serve', '--addr', `127.0.0.1:${rp}`], /relay listening/);
  }
  const m = await run(
    ['agent', '--relay', relay, '--code-file', join(dir, 'code'), '--http', `http://127.0.0.1:${upstream.address().port}`, '--name', 'go-agent'],
    /(http:\/\/127\.0\.0\.1:\d+\/#k=[A-Za-z0-9_-]+)[\s\S]*connected to relay/
  );
  goConn = m[1];
});

after(() => {
  for (const p of procs) p.kill();
  wss?.close();
  upstream?.close();
});

test('JS client → Go relay → Go agent → HTTP upstream', async () => {
  const c = new Client(goConn);
  const r = await c.fetch('/hello');
  assert.equal(r.status, 200);
  assert.equal(await r.text(), 'hello from node upstream');
  assert.equal(c.path, 'relay');

  const big = crypto.getRandomValues(new Uint8Array(1 << 16)).map((x, i) => x ^ i);
  const body = new Uint8Array(2 << 20);
  for (let i = 0; i < body.length; i += big.length) body.set(big.subarray(0, Math.min(big.length, body.length - i)), i);
  const e = await c.fetch('/echo', { method: 'POST', body });
  const got = new Uint8Array(await e.arrayBuffer());
  assert.equal(got.length, body.length);
  assert.deepEqual(got, body);

  const nf = await c.fetch('/missing');
  assert.equal(nf.status, 404);
  c.close();
});

test('JS client WebSocket through the Go agent to a real WebSocket server', async () => {
  const c = new Client(goConn);
  const ws = await c.websocket('/live');
  await ws.send('hi');
  assert.deepEqual(await ws.read(), { text: 'echo:hi' });
  await ws.send(new Uint8Array([1, 2, 3]));
  assert.deepEqual(await ws.read(), { data: new Uint8Array([1, 2, 3]) });
  await ws.close();
  c.close();
});

test('Go client (bolthole connect) → Go relay → JS agent', async () => {
  const agent = new Agent({
    relay,
    name: 'js-agent',
    fetch: async (req) => {
      const u = new URL(req.url);
      if (req.method === 'POST') return new Response(`len=${(await req.arrayBuffer()).byteLength}`);
      return Response.json({ path: u.pathname, q: u.searchParams.get('q'), ua: req.headers.get('x-test') });
    },
  });
  const connected = new Promise((r) => (agent.opts.onStatus = (s) => s.connected && r()));
  agent.run();
  await connected;

  const lp = port();
  await run(['connect', agent.connString, '--listen', `127.0.0.1:${lp}`], /Agent available/);
  const r = await fetch(`http://127.0.0.1:${lp}/api/thing?q=42`, { headers: { 'x-test': 'yes' } });
  assert.deepEqual(await r.json(), { path: '/api/thing', q: '42', ua: 'yes' });
  const p = await fetch(`http://127.0.0.1:${lp}/upload`, { method: 'POST', body: new Uint8Array(700_000) });
  assert.equal(await p.text(), 'len=700000');
  agent.stop();
});

test('JS client ↔ JS agent, custom stream kind, agent-initiated stream', async () => {
  const agent = new Agent({
    relay,
    kinds: {
      // A tiny RPC kind: uppercase whatever arrives.
      'example.upper': async (st) => {
        await st.accept({ ok: true });
        const text = new TextDecoder().decode(await st.readAll());
        await st.write(new TextEncoder().encode(text.toUpperCase()));
        await st.closeWrite();
      },
    },
    onSession: (session) => {
      // Push a greeting to every client that connects.
      const st = session.open('example.greet', { from: 'agent' });
      st.write(new TextEncoder().encode('welcome')).then(() => st.closeWrite());
    },
  });
  const connected = new Promise((r) => (agent.opts.onStatus = (s) => s.connected && r()));
  agent.run();
  await connected;

  const greeted = new Promise((resolve) => {
    globalThis.__onStream = async (st) => {
      await st.accept({});
      resolve({ kind: st.kind, meta: st.meta, text: new TextDecoder().decode(await st.readAll()) });
      await st.closeWrite();
    };
  });
  const c = new Client(agent.connString, { onStream: (st) => globalThis.__onStream(st) });
  const st = await c.open('example.upper');
  assert.deepEqual(st.acceptMeta, { ok: true });
  await st.write(new TextEncoder().encode('quiet please'));
  await st.closeWrite();
  assert.equal(new TextDecoder().decode(await st.readAll()), 'QUIET PLEASE');
  assert.deepEqual(await greeted, { kind: 'example.greet', meta: { from: 'agent' }, text: 'welcome' });

  // Plaintext through the relay is refused for an E2E-only agent.
  const plain = await fetch(`${await agent.publicURL()}anything`);
  assert.equal(plain.status, 403);
  c.close();
  agent.stop();
});

test('public JS agent is reachable with plain fetch', async () => {
  const agent = new Agent({ relay, public: true, fetch: () => new Response('public hello') });
  const connected = new Promise((r) => (agent.opts.onStatus = (s) => s.connected && r()));
  agent.run();
  await connected;
  const r = await fetch(`${await agent.publicURL()}x`);
  assert.equal(await r.text(), 'public hello');
  agent.stop();
});
