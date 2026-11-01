// Phone remote for your laptop: live stats and a "ping my laptop" button,
// from any browser, end-to-end encrypted.
//
//   node examples/phone-remote/agent.mjs https://relay.example.com
//
// Open the printed link on your phone. The page itself is served through the
// relay in plaintext (it holds no secrets); the code in the link's #fragment
// never leaves the phone, and everything after the page loads runs over an
// E2E session the relay cannot read.
import { readFile } from 'node:fs/promises';
import os from 'node:os';
import { Agent } from '../../js/src/index.js';

const relay = process.argv[2] || process.env.BOLTHOLE_RELAY;
if (!relay) {
  console.error('usage: node agent.mjs <relay-url>');
  process.exit(2);
}
const here = new URL('.', import.meta.url);
const sdk = new URL('../../js/src/', import.meta.url);

function stats() {
  const [l1] = os.loadavg();
  return {
    host: os.hostname(),
    load: +(l1 / os.cpus().length).toFixed(2),
    memUsed: 1 - os.freemem() / os.totalmem(),
    uptime: os.uptime(),
    time: Date.now(),
  };
}

// Plaintext requests can only fetch the page and the SDK. Everything that
// touches the laptop requires the encrypted session.
async function publicFiles(req) {
  const { pathname } = new URL(req.url);
  if (pathname === '/' || pathname === '/index.html') {
    return new Response(await readFile(new URL('index.html', here)), { headers: { 'content-type': 'text/html; charset=utf-8' } });
  }
  const m = pathname.match(/^\/sdk\/([a-z]+\.js)$/);
  if (m) {
    try {
      return new Response(await readFile(new URL(m[1], sdk)), { headers: { 'content-type': 'text/javascript' } });
    } catch {}
  }
  return new Response('not found', { status: 404 });
}

async function privateAPI(req) {
  const { pathname } = new URL(req.url);
  if (pathname === '/stats') {
    // Server-sent events: one sample per second until the phone goes away.
    let timer;
    const body = new ReadableStream({
      start(ctrl) {
        const push = () => ctrl.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(stats())}\n\n`));
        push();
        timer = setInterval(push, 1000);
      },
      cancel() {
        clearInterval(timer);
      },
    });
    return new Response(body, { headers: { 'content-type': 'text/event-stream' } });
  }
  if (pathname === '/ping') return Response.json({ pong: Date.now() });
  if (pathname === '/notify' && req.method === 'POST') {
    const { text } = await req.json();
    const msg = String(text || '👋').slice(0, 200);
    process.stdout.write(`\x07\n\x1b[7m  📱 ${msg}  \x1b[0m\n\n`);
    return Response.json({ shown: true });
  }
  return new Response('not found', { status: 404 });
}

const agent = new Agent({
  relay,
  name: os.hostname(),
  public: true,
  fetch: (req, info) => (info.encrypted ? privateAPI(req) : publicFiles(req)),
  onStatus: (s) => !s.connected && s.error && console.error('relay:', s.error.message),
});
agent.run();
const page = await agent.publicURL();
console.log(`\nOpen on your phone:\n\n  ${page}#k=${agent.code}\n`);
