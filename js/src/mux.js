// Stream multiplexer (spec §3): framed, flow-controlled byte streams over
// any message transport with read()/write()/close().
import { concat, enc, dec, u32, readU32, deferred } from './bytes.js';

export const T = { OPEN: 1, ACCEPT: 2, DATA: 3, FIN: 4, RESET: 5, WINDOW: 6, PING: 7, PONG: 8, GOAWAY: 9 };
export const INITIAL_WINDOW = 256 * 1024;
export const MAX_CHUNK = 32 * 1024;
export const DEFAULT_MAX_FRAME = (1 << 20) + 5;

export function encodeFrame(type, id, payload = new Uint8Array(0)) {
  const b = new Uint8Array(5 + payload.length);
  b[0] = type;
  new DataView(b.buffer).setUint32(1, id >>> 0);
  b.set(payload, 5);
  return b;
}

export function decodeFrame(b) {
  if (b.length < 5) throw new Error('bolthole: short frame');
  return { type: b[0], id: readU32(b, 1), payload: b.subarray(5) };
}

const json = (v) => enc.encode(JSON.stringify(v));
const parse = (b) => (b.length ? JSON.parse(dec.decode(b)) : {});

/** Error carried by RESET (and GOAWAY). */
export class ResetError extends Error {
  constructor(code, message = '') {
    super(message ? `stream reset: ${code}: ${message}` : `stream reset: ${code}`);
    this.code = code;
  }
}

export class Session {
  /**
   * @param {{read(): Promise<Uint8Array>, write(b: Uint8Array): Promise<void>|void, close(): void}} conn
   * @param {{dialer: boolean, heartbeatMs?: number, maxStreams?: number, maxFrame?: number, onStream?: (s: Stream) => void}} opts
   */
  constructor(conn, opts) {
    this.conn = conn;
    this.dialer = !!opts.dialer;
    this.heartbeatMs = opts.heartbeatMs ?? 20000;
    this.maxStreams = opts.maxStreams ?? 256;
    this.maxFrame = opts.maxFrame ?? DEFAULT_MAX_FRAME;
    this.onStream = opts.onStream ?? ((s) => s.reset('unsupported_kind'));
    this.streams = new Map();
    this.nextId = this.dialer ? 1 : 2;
    this.closed = deferred();
    this.isClosed = false;
    this.err = null;
    this.remoteGoAway = false;
    this.localGoAway = false;
    this.onGoAway = null;
    this.lastRecv = Date.now();
    this.lastSend = Date.now();
    this.tail = Promise.resolve();
    this.readLoop();
    if (this.heartbeatMs > 0) {
      this.timer = setInterval(() => this.tick(), this.heartbeatMs / 4);
      this.timer.unref?.();
    }
  }

  /** Resolves when the session ends. */
  get done() {
    return this.closed.promise;
  }

  close(err = new Error('bolthole: session closed')) {
    if (this.isClosed) return;
    this.isClosed = true;
    this.err = err;
    clearInterval(this.timer);
    try {
      this.conn.close();
    } catch {}
    for (const st of this.streams.values()) st._terminate(err);
    this.streams.clear();
    this.closed.resolve(err);
  }

  tick() {
    const now = Date.now();
    if (now - this.lastRecv > 3 * this.heartbeatMs) return this.close(new Error('bolthole: peer silent for too long'));
    if (now - this.lastSend >= this.heartbeatMs) this.send(T.PING, 0, u32(now % 2 ** 32)).catch(() => {});
  }

  /** Serialised write of one frame. */
  send(type, id, payload) {
    if (this.isClosed) return Promise.reject(this.err);
    const frame = encodeFrame(type, id, payload);
    const p = this.tail.then(() => {
      if (this.isClosed) throw this.err;
      return this.conn.write(frame);
    });
    this.tail = p.then(
      () => (this.lastSend = Date.now()),
      (e) => this.close(e instanceof Error ? e : new Error(String(e)))
    );
    return p;
  }

  goAway(code = 'going_away', message = '') {
    if (this.localGoAway) return;
    this.localGoAway = true;
    this.send(T.GOAWAY, 0, json({ code, message })).catch(() => {});
  }

  /**
   * Opens a stream; does not wait for ACCEPT (use stream.accepted).
   * @returns {Stream}
   */
  open(kind, meta) {
    if (this.isClosed) throw this.err;
    if (this.remoteGoAway) throw new Error('bolthole: peer is going away');
    const id = this.nextId;
    this.nextId += 2;
    const payload = json(meta === undefined ? { kind } : { kind, meta });
    if (payload.length + 5 > this.maxFrame) throw new Error('bolthole: open meta too large');
    const st = new Stream(this, id, kind, meta ?? null, true);
    this.streams.set(id, st);
    this.send(T.OPEN, id, payload).catch(() => {});
    return st;
  }

  async readLoop() {
    try {
      for (;;) {
        const msg = await this.conn.read();
        this.lastRecv = Date.now();
        if (msg.length > this.maxFrame) throw new Error('bolthole: frame too large');
        this.handle(decodeFrame(msg));
      }
    } catch (e) {
      this.close(e instanceof Error ? e : new Error(String(e)));
    }
  }

  isPeerId(id) {
    return id !== 0 && (id % 2 === 1) !== this.dialer;
  }

  handle(f) {
    switch (f.type) {
      case T.PING:
        this.send(T.PONG, 0, f.payload.slice()).catch(() => {});
        return;
      case T.PONG:
        return;
      case T.GOAWAY:
        this.remoteGoAway = true;
        this.onGoAway?.(parse(f.payload));
        return;
      case T.OPEN: {
        if (!this.isPeerId(f.id)) throw new Error('bolthole: peer opened a stream with our parity');
        if (this.streams.has(f.id)) throw new Error('bolthole: stream opened twice');
        let op;
        try {
          op = parse(f.payload);
        } catch {
          op = {};
        }
        if (!op.kind) return void this.send(T.RESET, f.id, json({ code: 'protocol_error' })).catch(() => {});
        if (this.localGoAway) return void this.send(T.RESET, f.id, json({ code: 'going_away' })).catch(() => {});
        if (this.streams.size >= this.maxStreams) return void this.send(T.RESET, f.id, json({ code: 'refused' })).catch(() => {});
        const st = new Stream(this, f.id, op.kind, op.meta ?? null, false);
        this.streams.set(f.id, st);
        try {
          this.onStream(st);
        } catch (e) {
          st.reset('internal', String(e?.message ?? e));
        }
        return;
      }
      case T.ACCEPT:
      case T.DATA:
      case T.FIN:
      case T.RESET:
      case T.WINDOW: {
        const st = this.streams.get(f.id);
        if (!st) {
          if (f.type === T.ACCEPT || f.type === T.DATA) this.send(T.RESET, f.id, json({ code: 'unknown_stream' })).catch(() => {});
          return;
        }
        st._frame(f);
        return;
      }
      default:
        return; // unknown frame types are ignored
    }
  }
}

export class Stream {
  constructor(session, id, kind, meta, outbound) {
    this.session = session;
    this.id = id;
    this.kind = kind;
    this.meta = meta;
    this.outbound = outbound;
    this.buf = [];
    this.bufLen = 0;
    this.recvFin = false;
    this.recvCredit = INITIAL_WINDOW;
    this.unacked = 0;
    this.sendCredit = INITIAL_WINDOW;
    this.localFin = false;
    this.isAccepted = false;
    this.error = null;
    this.finished = false;
    this._accept = deferred();
    this._accept.promise.catch(() => {});
    this._done = deferred();
    this._wake = deferred();
  }

  /** Resolves with the peer's ACCEPT meta; rejects on reset. */
  get accepted() {
    return this._accept.promise;
  }

  /** Resolves when the stream is fully finished or reset. */
  get done() {
    return this._done.promise;
  }

  _signal() {
    const w = this._wake;
    this._wake = deferred();
    w.resolve();
  }

  accept(meta) {
    if (this.outbound || this.isAccepted || this.error) return Promise.resolve();
    this.isAccepted = true;
    return this.session.send(T.ACCEPT, this.id, meta === undefined ? new Uint8Array(0) : json(meta));
  }

  /** Reads the next chunk; resolves null at end of stream. */
  async read() {
    for (;;) {
      if (this.bufLen > 0) {
        const chunk = this.buf.shift();
        this.bufLen -= chunk.length;
        this.unacked += chunk.length;
        if (this.unacked >= INITIAL_WINDOW / 2 && !this.recvFin && !this.error) {
          const credit = this.unacked;
          this.unacked = 0;
          this.recvCredit += credit;
          this.session.send(T.WINDOW, this.id, u32(credit)).catch(() => {});
        }
        return chunk;
      }
      if (this.error) throw this.error;
      if (this.recvFin) return null;
      await this._wake.promise;
    }
  }

  /** Reads everything until FIN. */
  async readAll() {
    const parts = [];
    for (let c; (c = await this.read()) !== null; ) parts.push(c);
    return concat(...parts);
  }

  /** Writes bytes, waiting for flow-control credit as needed. */
  async write(data) {
    if (!this.outbound && !this.isAccepted) await this.accept();
    let off = 0;
    while (off < data.length) {
      if (this.error) throw this.error;
      if (this.localFin) throw new Error('bolthole: stream closed');
      if (this.sendCredit <= 0) {
        await this._wake.promise;
        continue;
      }
      const n = Math.min(data.length - off, MAX_CHUNK, this.sendCredit);
      this.sendCredit -= n;
      await this.session.send(T.DATA, this.id, data.subarray(off, off + n));
      off += n;
    }
  }

  /** Half-close: no more data from this side. */
  async closeWrite() {
    if (this.localFin || this.error) return;
    if (!this.outbound && !this.isAccepted) await this.accept();
    this.localFin = true;
    this._signal();
    await this.session.send(T.FIN, this.id);
    this._maybeFinish();
  }

  /** Done with the stream; resets it if the peer is still sending. */
  close() {
    if (!this.recvFin && !this.error) return this.reset('cancel');
    return this.closeWrite();
  }

  reset(code = 'cancel', message = '') {
    if (this.error || this.finished) return;
    this.session.send(T.RESET, this.id, json({ code, message })).catch(() => {});
    this._terminate(new ResetError(code, message));
  }

  _terminate(err) {
    if (!this.error && !this.finished) this.error = err;
    const was = this.finished;
    this.finished = true;
    this._accept.reject(err);
    this._signal();
    if (!was) this._done.resolve();
    this.session.streams.delete(this.id);
  }

  _maybeFinish() {
    if (this.localFin && this.recvFin && !this.finished) {
      this.finished = true;
      this._signal();
      this._done.resolve();
      this.session.streams.delete(this.id);
    }
  }

  _frame(f) {
    switch (f.type) {
      case T.ACCEPT:
        if (this.outbound && !this.isAccepted) {
          this.isAccepted = true;
          let meta = {};
          try {
            meta = parse(f.payload);
          } catch {}
          this._accept.resolve(meta);
        }
        return;
      case T.DATA:
        if (this.recvFin || this.error) return;
        if (f.payload.length > this.recvCredit) return this.reset('flow_control', 'window exceeded');
        this.recvCredit -= f.payload.length;
        this.buf.push(f.payload.slice());
        this.bufLen += f.payload.length;
        this._signal();
        return;
      case T.FIN:
        this.recvFin = true;
        this._signal();
        this._maybeFinish();
        return;
      case T.RESET: {
        let r = {};
        try {
          r = parse(f.payload);
        } catch {}
        this._terminate(new ResetError(r.code || 'internal', r.message || ''));
        return;
      }
      case T.WINDOW:
        if (f.payload.length !== 4) return this.reset('protocol_error', 'bad window');
        this.sendCredit += readU32(f.payload);
        this._signal();
        return;
    }
  }

  /** The incoming bytes as a web ReadableStream. */
  readable() {
    return new ReadableStream({
      pull: async (ctrl) => {
        try {
          const c = await this.read();
          if (c === null) ctrl.close();
          else ctrl.enqueue(c);
        } catch (e) {
          ctrl.error(e);
        }
      },
      cancel: () => this.close(),
    });
  }

  /** Pumps a web ReadableStream (or async iterable) into this stream, then FINs. */
  async pipeFrom(source) {
    try {
      if (source) {
        for await (const chunk of source) await this.write(chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk));
      }
      await this.closeWrite();
    } catch (e) {
      this.reset('cancel', String(e?.message ?? ''));
    }
  }
}
