// Agent: expose a fetch-style handler (and optional WebSocket / custom
// stream handlers) through a relay, end-to-end encrypted. Runs anywhere
// with WebSocket + WebCrypto: Node 22+, Deno, Bun.
import { derive, formatConnString, newCode } from './codes.js';
import { agentHandshake, PROTO_MUX, MAX_RECORD } from './noise.js';
import { Session } from './mux.js';
import { wsConn, serveHttp, WsStream } from './kinds.js';
import { enc, dec } from './bytes.js';
import { VERSION } from './version.js';

const E2E_PATH = '/.bolthole/e2e';

/**
 * @typedef {object} AgentOptions
 * @property {string} relay                       relay base URL
 * @property {string} [code]                      connect code (generated if omitted)
 * @property {(req: Request, info: {encrypted: boolean, session: Session}) => Response|Promise<Response>} [fetch] handler for `http` streams
 * @property {(ws: WsStream, meta: object) => void} [websocket] handler for `ws` streams
 * @property {Record<string, (stream: import('./mux.js').Stream, info: object) => void>} [kinds] custom stream kinds
 * @property {(session: Session, hello: object) => void} [onSession] each connected client
 * @property {string} [name]
 * @property {string} [auth]                      registration key or JWT
 * @property {boolean} [public]                   also serve plaintext streams from the relay
 * @property {string[]} [direct]                  direct URLs to advertise to clients
 * @property {(s: {connected: boolean, error?: Error, retryMs?: number}) => void} [onStatus]
 * @property {typeof WebSocket} [WebSocket]
 */

export class Agent {
  /** @param {AgentOptions} opts */
  constructor(opts) {
    this.opts = opts;
    this.code = opts.code ?? newCode();
    this.WS = opts.WebSocket ?? globalThis.WebSocket;
    this.keys = derive(this.code);
    this.stopped = false;
    this.outer = null;
  }

  /** The connection string clients need. */
  get connString() {
    return formatConnString(this.opts.relay, this.code);
  }

  /** Where plaintext traffic lands when `public` is set. */
  async publicURL() {
    return `${this.opts.relay.replace(/\/+$/, '')}/c/${(await this.keys).routingToken}/`;
  }

  /** Connects and keeps reconnecting until stop(). Resolves when stopped. */
  async run() {
    let attempt = 0;
    while (!this.stopped) {
      const started = Date.now();
      try {
        const sess = await this._connect();
        attempt = 0;
        this.opts.onStatus?.({ connected: true });
        await Promise.race([
          sess.done,
          new Promise((r) => (sess.onGoAway = r)), // relay draining: redial now
        ]);
        if (sess.remoteGoAway && !this.stopped) continue;
        throw sess.err ?? new Error('bolthole: relay connection closed');
      } catch (e) {
        if (this.stopped) break;
        if (e.permanent) {
          this.opts.onStatus?.({ connected: false, error: e });
          throw e;
        }
        if (Date.now() - started > 30000) attempt = 0;
        let delay = Math.random() * Math.min(30000, 500 * 2 ** attempt) + 250;
        if (e.code === 4000) delay = 30000; // replaced by another agent with our code
        attempt++;
        this.opts.onStatus?.({ connected: false, error: e, retryMs: delay });
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  stop() {
    this.stopped = true;
    this.outer?.goAway('going_away', 'agent shutting down');
    this.outer?.close();
  }

  async _connect() {
    const { tokenHash } = await this.keys;
    const base = this.opts.relay.replace(/^http/, 'ws').replace(/\/+$/, '');
    const conn = await wsConn(new this.WS(`${base}/v1/agent?th=${tokenHash}`, ['relay.v1']));
    await conn.write(JSON.stringify({ type: 'hello', v: 1, token_hash: tokenHash, name: this.opts.name, auth: this.opts.auth, version: VERSION, caps: ['mux/1'] }));
    let w;
    try {
      w = JSON.parse(dec.decode(await conn.read()));
    } catch (e) {
      if (e.code === 4002 || e.code === 4003) e.permanent = true;
      throw e;
    }
    if (w.type === 'error') {
      const err = new Error(`bolthole: relay refused registration: ${w.code}: ${w.message}`);
      err.permanent = w.code === 'unauthorized' || w.code === 'invalid_token_hash';
      conn.close();
      throw err;
    }
    const sess = new Session(conn, { dialer: true, heartbeatMs: w.heartbeat_ms || 20000, onStream: (st) => this._outer(st) });
    this.outer = sess;
    return sess;
  }

  _outer(st) {
    if (st.kind === 'ws' && (st.meta?.path || '').split('?')[0] === E2E_PATH) {
      st.accept({}).then(() => this.serveSecure(new WsStream(st).asConn())).catch(() => st.reset('internal'));
      return;
    }
    if (!this.opts.public) return st.reset('e2e_required', 'this agent only accepts end-to-end encrypted sessions');
    this._dispatch(st, { encrypted: false, session: this.outer });
  }

  /** Runs the E2E handshake on any transport (relay stream or direct socket). */
  async serveSecure(conn) {
    const { psk } = await this.keys;
    let res;
    try {
      res = await agentHandshake(conn, psk, (h) => {
        if (!(h.protos || []).includes(PROTO_MUX)) throw new Error('no common protocol');
        return { proto: PROTO_MUX, name: this.opts.name, direct: this.opts.direct };
      });
    } catch {
      conn.close();
      return;
    }
    const inner = new Session(res.channel, { dialer: false, maxFrame: MAX_RECORD, onStream: (st) => this._dispatch(st, { encrypted: true, session: inner }) });
    this.opts.onSession?.(inner, res.hello);
    await inner.done;
  }

  _dispatch(st, info) {
    const o = this.opts;
    if (st.kind === 'http' && o.fetch) return void serveHttp(st, o.fetch, info).catch(() => st.reset('internal'));
    if (st.kind === 'ws' && o.websocket) {
      return void st
        .accept({})
        .then(() => o.websocket(new WsStream(st), st.meta || {}, info))
        .catch(() => st.reset('internal'));
    }
    const h = o.kinds?.[st.kind];
    if (h) return void Promise.resolve()
      .then(() => h(st, info))
      .catch(() => st.reset('internal'));
    st.reset('unsupported_kind', st.kind);
  }
}

/** Shorthand: start an agent and return it. */
export function serve(opts) {
  const a = new Agent(opts);
  a.running = a.run();
  a.running.catch(() => {});
  return a;
}

export { enc };
