// Small byte helpers shared by every module. No dependencies.

export const enc = new TextEncoder();
export const dec = new TextDecoder();

/** @param {...Uint8Array} parts */
export function concat(...parts) {
  let n = 0;
  for (const p of parts) n += p.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

export function b64url(bytes) {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export function unb64url(s) {
  const t = s.replace(/-/g, '+').replace(/_/g, '/');
  const bin = atob(t + '='.repeat((4 - (t.length % 4)) % 4));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function hex(bytes) {
  let s = '';
  for (const b of bytes) s += b.toString(16).padStart(2, '0');
  return s;
}

export function u32(n) {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n >>> 0);
  return b;
}

export function readU32(b, off = 0) {
  return new DataView(b.buffer, b.byteOffset, b.byteLength).getUint32(off);
}

/** Normalise ArrayBuffer / views / Node Buffers to a plain Uint8Array. */
export function toBytes(x) {
  if (x instanceof Uint8Array) return x;
  if (x instanceof ArrayBuffer) return new Uint8Array(x);
  if (ArrayBuffer.isView(x)) return new Uint8Array(x.buffer, x.byteOffset, x.byteLength);
  if (typeof x === 'string') return enc.encode(x);
  throw new TypeError('expected bytes');
}

/** A promise you can resolve from outside. */
export function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** An unbounded async FIFO: push() items, await shift(). */
export class Queue {
  constructor() {
    this.items = [];
    this.waiters = [];
    this.error = null;
  }
  push(item) {
    if (this.error) return;
    const w = this.waiters.shift();
    if (w) w.resolve(item);
    else this.items.push(item);
  }
  fail(err) {
    if (this.error) return;
    this.error = err;
    for (const w of this.waiters.splice(0)) w.reject(err);
  }
  shift() {
    if (this.items.length) return Promise.resolve(this.items.shift());
    if (this.error) return Promise.reject(this.error);
    const d = deferred();
    this.waiters.push(d);
    return d.promise;
  }
}
