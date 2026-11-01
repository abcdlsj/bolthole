// Stream kinds (spec §4): http, ws, tcp — and helpers to adapt WebSockets
// into the read()/write()/close() transports the rest of the SDK uses.
import { concat, enc, dec, u32, readU32, Queue, toBytes } from './bytes.js';

export const OP_TEXT = 1;
export const OP_BINARY = 2;
export const OP_CLOSE = 8;
const MAX_RECORD = 16 << 20;

const HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'upgrade', 'te', 'trailer', 'proxy-connection', 'proxy-authenticate', 'proxy-authorization', 'host']);

/** Headers (or a plain object) → spec header map: lowercase names, array values. */
export function encodeHeaders(h) {
  const out = {};
  const add = (k, v) => {
    const lk = k.toLowerCase();
    if (HOP.has(lk) || lk.startsWith('sec-websocket-')) return;
    (out[lk] ||= []).push(String(v));
  };
  if (h instanceof Headers) h.forEach((v, k) => add(k, v));
  else if (h) for (const [k, v] of Object.entries(h)) for (const x of [].concat(v)) add(k, x);
  return out;
}

export function decodeHeaders(m) {
  const h = new Headers();
  for (const [k, vs] of Object.entries(m || {})) {
    if (HOP.has(k.toLowerCase())) continue;
    for (const v of vs) h.append(k, v);
  }
  return h;
}

/**
 * Message view of a `ws` stream: read() → {text?: string, data?: Uint8Array}
 * or null after the peer closes; send(string|bytes); close(code, reason).
 */
export class WsStream {
  constructor(stream) {
    this.stream = stream;
    this.pending = new Uint8Array(0);
    this.closeInfo = null;
  }

  async _need(n) {
    while (this.pending.length < n) {
      const c = await this.stream.read();
      if (c === null) return false;
      this.pending = this.pending.length ? concat(this.pending, c) : c;
    }
    return true;
  }

  /** @returns {Promise<{text?: string, data?: Uint8Array} | null>} */
  async read() {
    for (;;) {
      if (!(await this._need(5))) return null;
      const op = this.pending[0];
      const n = readU32(this.pending, 1);
      if (n > MAX_RECORD) {
        this.stream.reset('protocol_error', 'ws record too large');
        throw new Error('bolthole: ws record too large');
      }
      if (!(await this._need(5 + n))) return null;
      const payload = this.pending.slice(5, 5 + n);
      this.pending = this.pending.subarray(5 + n);
      if (op === OP_TEXT) return { text: dec.decode(payload) };
      if (op === OP_BINARY) return { data: payload };
      if (op === OP_CLOSE) {
        const code = payload.length >= 2 ? (payload[0] << 8) | payload[1] : 1005;
        this.closeInfo = { code, reason: dec.decode(payload.subarray(2)) };
        return null;
      }
    }
  }

  send(msg) {
    const text = typeof msg === 'string';
    const payload = text ? enc.encode(msg) : toBytes(msg);
    return this.stream.write(concat(new Uint8Array([text ? OP_TEXT : OP_BINARY]), u32(payload.length), payload));
  }

  async close(code = 1000, reason = '') {
    const r = enc.encode(reason).subarray(0, 123);
    const p = concat(new Uint8Array([code >> 8, code & 0xff]), r);
    try {
      await this.stream.write(concat(new Uint8Array([OP_CLOSE]), u32(p.length), p));
      await this.stream.closeWrite();
    } catch {}
  }

  /** Transport view (binary messages) for running E2E on top of it. */
  asConn() {
    return {
      read: async () => {
        const m = await this.read();
        if (!m) throw new Error('bolthole: ws stream closed');
        return m.data ?? enc.encode(m.text);
      },
      write: (b) => this.send(b),
      close: () => {
        this.close().finally(() => this.stream.reset('cancel'));
      },
    };
  }
}

/**
 * Adapts a WebSocket (browser, Node 22+, Deno, Bun, Workers, or the `ws`
 * package) into read()/write()/close(). Resolves once open.
 */
export function wsConn(ws) {
  try {
    ws.binaryType = 'arraybuffer';
  } catch {} // not settable everywhere (e.g. Workers); bytes still arrive as ArrayBuffer
  const q = new Queue();
  const onMessage = (ev) => {
    const d = ev && ev.data !== undefined ? ev.data : ev;
    if (typeof d === 'string') q.push(enc.encode(d));
    else q.push(toBytes(d));
  };
  const onClose = (ev) => q.fail(Object.assign(new Error(`bolthole: websocket closed (${ev?.code ?? ''})`), { code: ev?.code }));
  ws.addEventListener('message', onMessage);
  ws.addEventListener('close', onClose);
  ws.addEventListener('error', () => q.fail(new Error('bolthole: websocket error')));
  const conn = {
    ws,
    read: () => q.shift(),
    write: (b) => {
      if (ws.readyState !== 1) return Promise.reject(new Error('bolthole: websocket not open'));
      ws.send(b);
      return Promise.resolve();
    },
    close: (code = 1000, reason = '') => {
      try {
        ws.close(code, reason);
      } catch {}
    },
  };
  if (ws.readyState === 1) return Promise.resolve(conn);
  return new Promise((resolve, reject) => {
    ws.addEventListener('open', () => resolve(conn), { once: true });
    ws.addEventListener('close', (ev) => reject(Object.assign(new Error(`bolthole: websocket failed (${ev?.code ?? ''})`), { code: ev?.code })), { once: true });
    ws.addEventListener('error', () => reject(new Error('bolthole: websocket failed')), { once: true });
  });
}

const NULL_BODY = new Set([101, 103, 204, 205, 304]);

/** Client side of an `http` stream: fetch() semantics over a mux session. */
export async function fetchOver(session, input, init) {
  // Paths like "/api" are fine: the host part never leaves this process.
  if (typeof input === 'string' && input.startsWith('/')) input = 'http://agent' + input;
  const req = new Request(input, init);
  const u = new URL(req.url);
  const headers = encodeHeaders(req.headers);
  const st = session.open('http', { method: req.method, path: u.pathname + u.search, headers });
  const abort = () => st.reset('cancel');
  req.signal?.addEventListener('abort', abort, { once: true });
  st.pipeFrom(req.body);
  let meta;
  try {
    meta = await st.accepted;
  } catch (e) {
    req.signal?.removeEventListener('abort', abort);
    throw e;
  }
  const body = NULL_BODY.has(meta.status) || req.method === 'HEAD' ? null : st.readable();
  if (!body) st.close();
  return new Response(body, { status: meta.status, headers: decodeHeaders(meta.headers) });
}

/** Agent side of an `http` stream: answer it with a fetch-style handler. */
export async function serveHttp(st, handler, info = {}) {
  const m = st.meta || {};
  if (!m.method || typeof m.path !== 'string' || !m.path.startsWith('/')) return st.reset('protocol_error', 'bad http meta');
  const hasBody = !['GET', 'HEAD'].includes(m.method);
  const req = new Request('http://agent' + m.path, {
    method: m.method,
    headers: decodeHeaders(m.headers),
    body: hasBody ? st.readable() : null,
    duplex: 'half',
  });
  let res;
  try {
    res = await handler(req, info);
  } catch (e) {
    res = new Response('internal error', { status: 500 });
  }
  if (!(res instanceof Response)) res = new Response(String(res ?? ''), { status: 200 });
  await st.accept({ status: res.status, headers: encodeHeaders(res.headers) });
  await st.pipeFrom(res.body);
}
