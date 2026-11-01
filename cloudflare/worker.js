// bolthole relay, Cloudflare Workers edition: no server to run.
//
// Every agent gets its own Durable Object, named by its token hash, and every
// client request for that agent is routed to the same object. The object
// runs the same RelayHub as any other JS relay.
import { RelayHub, authFromEnv, parseClientPath, isTokenHash, messageConn, VERSION } from '../js/src/relay.js';
import { wsConn } from '../js/src/kinds.js';
import { hashToken } from '../js/src/codes.js';

const text = (s, status = 200) => new Response(s, { status, headers: { 'content-type': 'text/plain; charset=utf-8' } });
const json = (status, body) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/healthz') return json(200, { ok: true, edition: 'cloudflare', version: VERSION });
    if (url.pathname === '/') return text(`bolthole relay ${VERSION} (Cloudflare) is running.\n`);

    let hash;
    if (url.pathname === '/v1/agent') {
      hash = url.searchParams.get('th') || '';
      if (!isTokenHash(hash)) return json(400, { error: 'invalid_token_hash' });
    } else {
      const parsed = parseClientPath(request.url);
      if (!parsed) return json(404, { error: 'not_found' });
      hash = await hashToken(parsed.token);
    }
    const ns = env.AGENTS;
    const opts = env.LOCATION_HINT ? { locationHint: env.LOCATION_HINT } : undefined;
    const stub = ns.get(ns.idFromName(hash), opts);
    return stub.fetch(request);
  },
};

export class AgentRoom {
  constructor(state, env) {
    this.state = state;
    this.hub = new RelayHub({
      auth: authFromEnv({ jwtSecret: env.BOLTHOLE_JWT_SECRET, keys: env.BOLTHOLE_KEYS }),
      log: (m) => console.log(m),
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get('upgrade')?.toLowerCase() === 'websocket';

    if (url.pathname === '/v1/agent') {
      if (!upgrade) return json(426, { error: 'websocket required' });
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      const conn = await wsConn(server);
      this.state.waitUntil(this.hub.handleAgent(conn, url.searchParams.get('th')));
      const headers = request.headers.get('sec-websocket-protocol')?.includes('relay.v1') ? { 'sec-websocket-protocol': 'relay.v1' } : {};
      return new Response(null, { status: 101, webSocket: client, headers });
    }

    const parsed = parseClientPath(request.url);
    const hash = await hashToken(parsed.token);
    if (!upgrade) return this.hub.handleHTTP(request, hash, parsed.path);

    const res = await this.hub.openWebSocket(request, hash, parsed.path);
    if (res.response) return res.response;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    this.state.waitUntil(this.hub.bridge(messageConn(server), res.stream));
    const headers = res.protocol ? { 'sec-websocket-protocol': res.protocol } : {};
    return new Response(null, { status: 101, webSocket: client, headers });
  }
}
