// Client: reach an agent through a relay (or directly) and talk to it with
// fetch(), WebSockets, raw streams, or your own stream kinds.
import { parseConnString, derive } from './codes.js';
import { clientHandshake, PROTO_MUX, MAX_RECORD } from './noise.js';
import { Session } from './mux.js';
import { wsConn, fetchOver, WsStream } from './kinds.js';

const E2E_PATH = '/.bolthole/e2e';

/**
 * @typedef {object} ConnectOptions
 * @property {string} [name]          sent to the agent inside the encrypted hello
 * @property {'auto'|'relay'|'direct'} [mode]
 * @property {string[]} [direct]      extra direct candidates (ws://host:port/.bolthole/e2e)
 * @property {number} [relayDelayMs]  head start for direct candidates (250)
 * @property {number} [directTimeoutMs] (2000)
 * @property {(path: string) => void} [onPath]
 * @property {(stream: import('./mux.js').Stream) => void} [onStream] agent-opened streams
 * @property {typeof WebSocket} [WebSocket] constructor to use (defaults to the global)
 */

/**
 * A resilient connection to one agent. Reconnects on demand and remembers
 * direct candidates the agent advertises.
 */
export class Client {
  /** @param {string} connString @param {ConnectOptions} [opts] */
  constructor(connString, opts = {}) {
    this.cs = parseConnString(connString);
    this.opts = { mode: 'auto', relayDelayMs: 250, directTimeoutMs: 2000, ...opts };
    this.WS = opts.WebSocket ?? globalThis.WebSocket;
    if (!this.WS) throw new Error('bolthole: no WebSocket implementation; pass opts.WebSocket');
    this.direct = [...(opts.direct ?? [])];
    this.keys = derive(this.cs.code);
    this.current = null;
    this.dialing = null;
    this.path = '';
  }

  /** Returns a live session, dialing if needed. */
  async session() {
    if (this.current && !this.current.session.isClosed && !this.current.session.remoteGoAway) return this.current.session;
    if (!this.dialing) {
      this.dialing = this._race().finally(() => (this.dialing = null));
    }
    const c = await this.dialing;
    return c.session;
  }

  async _race() {
    const { routingToken, psk } = await this.keys;
    const cands = [];
    if (this.opts.mode !== 'relay') for (const url of this.direct) cands.push({ path: 'direct ' + url, url, delay: 0, timeout: this.opts.directTimeoutMs });
    if (this.opts.mode !== 'direct') {
      const base = this.cs.relay.replace(/^http/, 'ws');
      cands.push({ path: 'relay', url: `${base}/c/${routingToken}${E2E_PATH}`, delay: cands.length ? this.opts.relayDelayMs : 0, timeout: 30000 });
    }
    if (!cands.length) throw new Error('bolthole: no path to the agent');

    const ctrl = { done: false };
    const attempts = cands.map((c) => this._dialOne(c, psk, ctrl));
    let winner;
    try {
      winner = await Promise.any(attempts);
    } catch (e) {
      throw new AggregateError(e.errors, 'bolthole: could not reach the agent');
    }
    ctrl.done = true;
    // Close any loser that also finished.
    for (const a of attempts) a.then((c) => c !== winner && c.session.close(), () => {});
    this._install(winner);
    return winner;
  }

  async _dialOne(cand, psk, ctrl) {
    if (cand.delay) await new Promise((r) => setTimeout(r, cand.delay));
    if (ctrl.done) throw new Error('lost the race');
    const ws = new this.WS(cand.url);
    const timer = setTimeout(() => {
      try {
        ws.close();
      } catch {}
    }, cand.timeout);
    try {
      const raw = await wsConn(ws);
      const { channel, hello } = await clientHandshake(raw, psk, { protos: [PROTO_MUX], name: this.opts.name || undefined });
      clearTimeout(timer);
      const session = new Session(channel, { dialer: true, maxFrame: MAX_RECORD, onStream: this.opts.onStream });
      return { session, hello, path: cand.path };
    } catch (e) {
      clearTimeout(timer);
      try {
        ws.close();
      } catch {}
      throw e;
    }
  }

  _install(c) {
    const old = this.current;
    this.current = c;
    this.path = c.path;
    for (const u of c.hello.direct ?? []) if (!this.direct.includes(u)) this.direct.push(u);
    if (old) {
      old.session.goAway('going_away', 'client switched path');
      const drain = () => (old.session.streams.size === 0 ? old.session.close() : setTimeout(drain, 500));
      drain();
    }
    this.opts.onPath?.(c.path);
  }

  /** fetch() to the agent. Only the URL's path and query are used. */
  async fetch(input, init) {
    return fetchOver(await this.session(), input, init);
  }

  /** Opens a WebSocket to `path` on the agent. */
  async websocket(path, protocols = []) {
    const s = await this.session();
    const st = s.open('ws', { path, protocols });
    const meta = await st.accepted;
    const ws = new WsStream(st);
    ws.protocol = meta.protocol ?? '';
    return ws;
  }

  /** Opens a raw byte stream to a named TCP target on the agent. */
  async tcp(target = '') {
    const s = await this.session();
    const st = s.open('tcp', { target });
    await st.accepted;
    return st;
  }

  /** Opens a stream of your own kind. Resolves after the agent accepts. */
  async open(kind, meta) {
    const s = await this.session();
    const st = s.open(kind, meta);
    st.acceptMeta = await st.accepted;
    return st;
  }

  /** Tries direct candidates now and switches if one answers. */
  async tryUpgrade() {
    if (this.path !== 'relay' || !this.direct.length) return false;
    const { psk } = await this.keys;
    const ctrl = { done: false };
    try {
      const c = await Promise.any(this.direct.map((url) => this._dialOne({ path: 'direct ' + url, url, timeout: this.opts.directTimeoutMs }, psk, ctrl)));
      ctrl.done = true;
      this._install(c);
      return true;
    } catch {
      return false;
    }
  }

  close() {
    this.current?.session.close();
    this.current = null;
  }
}

/** Shorthand: new Client(connString, opts). */
export function connect(connString, opts) {
  return new Client(connString, opts);
}
