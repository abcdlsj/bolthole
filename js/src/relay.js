// Platform-neutral relay core. The Cloudflare Worker edition
// (cloudflare/worker.js) and any other JS runtime wrap it: they hand it
// sockets as read()/write()/close() objects and get back Responses.
//
// It speaks exactly the protocol the Go relay does (spec §2), so agents and
// clients cannot tell the editions apart.
import { Session, ResetError } from './mux.js';
import { WsStream, encodeHeaders, decodeHeaders } from './kinds.js';
import { enc, dec, unb64url, deferred } from './bytes.js';
import { VERSION } from './version.js';

export { VERSION };
const TOKEN_RE = /^[A-Za-z0-9_-]{16,128}$/;
const HASH_RE = /^[0-9a-f]{64}$/;

export const isToken = (s) => TOKEN_RE.test(s);
export const isTokenHash = (s) => HASH_RE.test(s);

/** Parses "/c/<token>/<rest>" → {token, path} (path keeps the query). */
export function parseClientPath(url) {
  const u = new URL(url);
  const m = u.pathname.match(/^\/c\/([^/]+)(\/.*)?$/);
  if (!m || !isToken(m[1])) return null;
  return { token: m[1], path: (m[2] || '/') + u.search };
}

const json = (status, body, headers = {}) =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', 'cache-control': 'no-store', ...headers } });

/** Maps a stream reset to an HTTP status (spec §2). */
export function resetStatus(err) {
  const code = err instanceof ResetError ? err.code : 'agent_unavailable';
  const status =
    { not_found: 404, forbidden: 403, e2e_required: 403, refused: 503, going_away: 503, overloaded: 503, timeout: 504 }[code] ?? 502;
  return [status, code];
}

/**
 * Registration policy from env-style settings:
 *  - jwtSecret: HS256 tokens with aud "bolthole"
 *  - keys: comma-separated shared keys
 *  - neither: open
 */
export function authFromEnv({ jwtSecret, keys } = {}) {
  if (jwtSecret) return async (hello) => verifyJWT(jwtSecret, hello.auth || '');
  const list = (keys || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (list.length) {
    return async (hello) => {
      for (const k of list) if (await timingSafeEqual(k, hello.auth || '')) return 'key';
      throw new Error('registration key required');
    };
  }
  return async () => '';
}

async function timingSafeEqual(a, b) {
  // Compare digests so the comparison time doesn't depend on the input.
  const [x, y] = await Promise.all([a, b].map((s) => crypto.subtle.digest('SHA-256', enc.encode(s))));
  const u = new Uint8Array(x);
  const v = new Uint8Array(y);
  let d = 0;
  for (let i = 0; i < u.length; i++) d |= u[i] ^ v[i];
  return d === 0;
}

export async function verifyJWT(secret, token, now = Date.now()) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('invalid relay token');
  const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
  const ok = await crypto.subtle.verify('HMAC', key, unb64url(parts[2]), enc.encode(parts[0] + '.' + parts[1]));
  if (!ok) throw new Error('invalid relay token');
  const hdr = JSON.parse(dec.decode(unb64url(parts[0])));
  const claims = JSON.parse(dec.decode(unb64url(parts[1])));
  const aud = [].concat(claims.aud ?? []);
  if (hdr.alg !== 'HS256' || !aud.includes('bolthole') || !claims.sub) throw new Error('invalid relay token');
  if (!claims.exp || now / 1000 >= claims.exp) throw new Error('relay token expired');
  return claims.sub;
}

/**
 * Tracks agents by token hash and bridges client traffic to them.
 */
export class RelayHub {
  /**
   * @param {{auth?: (hello: object) => Promise<string>, agentWaitMs?: number, headTimeoutMs?: number, heartbeatMs?: number, log?: (msg: string) => void}} [opts]
   */
  constructor(opts = {}) {
    this.auth = opts.auth ?? (async () => '');
    this.agentWaitMs = opts.agentWaitMs ?? 5000;
    this.headTimeoutMs = opts.headTimeoutMs ?? 120000;
    this.heartbeatMs = opts.heartbeatMs ?? 20000;
    this.log = opts.log ?? (() => {});
    /** @type {Map<string, {session: Session, conn: any, name: string}>} */
    this.agents = new Map();
    this.waiters = new Map();
  }

  /**
   * Serves one agent socket until it closes. `th` is the hash from the URL
   * (the Worker uses it to pick the Durable Object; it must match the hello).
   */
  async handleAgent(conn, th) {
    const fail = (code, errCode, message) => {
      conn.write(JSON.stringify({ type: 'error', code: errCode, message })).catch(() => {});
      conn.close(code, message);
    };
    let hello;
    try {
      const first = await Promise.race([conn.read(), new Promise((_, rej) => setTimeout(() => rej(new Error('hello timeout')), 10000))]);
      hello = JSON.parse(dec.decode(first));
    } catch {
      return conn.close(4001, 'expected hello');
    }
    if (hello.type !== 'hello' || hello.v !== 1) return fail(4001, 'protocol_error', 'expected hello v1');
    if (!isTokenHash(hello.token_hash) || (th && th !== hello.token_hash)) return fail(4002, 'invalid_token_hash', 'bad token_hash');
    let subject;
    try {
      subject = await this.auth(hello);
    } catch (e) {
      return fail(4003, 'unauthorized', e.message);
    }
    await conn.write(JSON.stringify({ type: 'welcome', v: 1, heartbeat_ms: this.heartbeatMs, max_streams: 256, max_frame: (1 << 20) + 5 }));
    const session = new Session(conn, { dialer: false, heartbeatMs: this.heartbeatMs, onStream: (st) => st.reset('unsupported_kind', 'relay services are not enabled') });
    const entry = { session, conn, name: String(hello.name || '').slice(0, 64) };
    const old = this.agents.get(hello.token_hash);
    this.agents.set(hello.token_hash, entry);
    if (old) {
      old.conn.close(4000, 'replaced by a newer connection');
      old.session.close();
    }
    for (const w of this.waiters.get(hello.token_hash) ?? []) w.resolve();
    this.waiters.delete(hello.token_hash);
    this.log(`agent registered ${entry.name}${subject ? ' for ' + subject : ''}`);
    await session.done;
    if (this.agents.get(hello.token_hash) === entry) this.agents.delete(hello.token_hash);
    this.log(`agent disconnected ${entry.name}`);
  }

  /** The live agent for hash, waiting up to agentWaitMs for one to (re)connect. */
  async lookup(hash) {
    const deadline = Date.now() + this.agentWaitMs;
    for (;;) {
      const a = this.agents.get(hash);
      if (a && !a.session.isClosed && !a.session.remoteGoAway) return a;
      const left = deadline - Date.now();
      if (left <= 0) return null;
      const d = deferred();
      if (!this.waiters.has(hash)) this.waiters.set(hash, []);
      this.waiters.get(hash).push(d);
      await Promise.race([d.promise, new Promise((r) => setTimeout(r, Math.min(left, 250)))]);
    }
  }

  /** Tunnels a plain HTTP request. */
  async handleHTTP(request, hash, path) {
    const a = await this.lookup(hash);
    if (!a) return json(502, { error: 'agent_offline' });
    const headers = encodeHeaders(request.headers);
    let st;
    try {
      st = a.session.open('http', { method: request.method, path, headers });
    } catch {
      return json(502, { error: 'agent_unavailable' });
    }
    request.signal?.addEventListener('abort', () => st.reset('cancel'), { once: true });
    st.pipeFrom(request.body);
    let timer;
    try {
      const meta = await Promise.race([
        st.accepted,
        new Promise((_, rej) => (timer = setTimeout(() => rej(new ResetError('timeout')), this.headTimeoutMs))),
      ]);
      clearTimeout(timer);
      if (!(meta.status >= 200 && meta.status <= 999)) return json(502, { error: 'bad_response' });
      const nullBody = [204, 205, 304].includes(meta.status) || request.method === 'HEAD';
      if (nullBody) st.close();
      return new Response(nullBody ? null : st.readable(), { status: meta.status, headers: decodeHeaders(meta.headers) });
    } catch (e) {
      clearTimeout(timer);
      st.reset(e instanceof ResetError && e.code === 'timeout' ? 'timeout' : 'cancel');
      const [status, code] = resetStatus(e);
      return json(status, { error: code });
    }
  }

  /**
   * First half of a tunneled WebSocket: asks the agent and returns either
   * {stream, protocol} to bridge, or {response} to send instead.
   */
  async openWebSocket(request, hash, path) {
    const a = await this.lookup(hash);
    if (!a) return { response: json(502, { error: 'agent_offline' }) };
    const protocols = (request.headers.get('sec-websocket-protocol') || '').split(',').map((s) => s.trim()).filter(Boolean);
    let st;
    try {
      st = a.session.open('ws', { path, headers: encodeHeaders(request.headers), protocols });
    } catch {
      return { response: json(502, { error: 'agent_unavailable' }) };
    }
    let timer;
    try {
      const meta = await Promise.race([st.accepted, new Promise((_, rej) => (timer = setTimeout(() => rej(new ResetError('timeout')), 30000)))]);
      clearTimeout(timer);
      return { stream: new WsStream(st), protocol: meta.protocol || '' };
    } catch (e) {
      clearTimeout(timer);
      st.reset('cancel');
      const [status, code] = resetStatus(e);
      return { response: json(status, { error: code }) };
    }
  }

  /** Second half: copy messages both ways between `conn` and the stream. */
  async bridge(conn, ws) {
    const up = (async () => {
      for (;;) {
        const m = await ws.read();
        if (!m) {
          const c = ws.closeInfo?.code;
          conn.close(c && c >= 1000 && c !== 1005 && c !== 1006 ? c : 1000, ws.closeInfo?.reason || '');
          return;
        }
        await conn.write(m.text ?? m.data, m.text !== undefined);
      }
    })().catch(() => conn.close(1011, 'upstream closed'));
    const down = (async () => {
      for (;;) {
        let m;
        try {
          m = await conn.readMessage();
        } catch (e) {
          await ws.close(e.code && e.code >= 1000 && e.code !== 1005 && e.code !== 1006 ? e.code : 1000, '');
          setTimeout(() => ws.stream.reset('cancel'), 5000);
          return;
        }
        await ws.send(m);
      }
    })().catch(() => ws.stream.reset('cancel'));
    await Promise.all([up, down]);
  }

  /** Sends GOAWAY to every agent (deploys, shutdown). */
  drain() {
    for (const a of this.agents.values()) a.session.goAway('going_away', 'relay restarting');
  }

  health(edition) {
    return json(200, { ok: true, edition, version: VERSION, agents: this.agents.size });
  }
}

/**
 * Adapts a WebSocket into the conn shape the hub uses for client bridges:
 * readMessage() resolves string|Uint8Array (keeping the text/binary
 * distinction) and write(data, isText).
 */
export function messageConn(ws) {
  const queue = [];
  const waiters = [];
  let closed = null;
  ws.addEventListener('message', (ev) => {
    const d = typeof ev.data === 'string' ? ev.data : new Uint8Array(ev.data);
    const w = waiters.shift();
    if (w) w.resolve(d);
    else queue.push(d);
  });
  ws.addEventListener('close', (ev) => {
    closed = Object.assign(new Error('closed'), { code: ev.code });
    for (const w of waiters.splice(0)) w.reject(closed);
  });
  return {
    readMessage() {
      if (queue.length) return Promise.resolve(queue.shift());
      if (closed) return Promise.reject(closed);
      const d = deferred();
      waiters.push(d);
      return d.promise;
    },
    async write(data) {
      ws.send(data);
    },
    close(code = 1000, reason = '') {
      try {
        ws.close(code, reason);
      } catch {}
    },
  };
}
