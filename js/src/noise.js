// Noise_NNpsk0_25519_AESGCM_SHA256 (https://noiseprotocol.org/noise.html)
// on WebCrypto, plus the encrypted message channel built from it (spec §5).
//
// Only the pieces this pattern needs are implemented: psk, e, ee tokens.
import { concat, enc, dec, Queue } from './bytes.js';

const subtle = globalThis.crypto.subtle;
const PROTOCOL = enc.encode('Noise_NNpsk0_25519_AESGCM_SHA256');
export const PROLOGUE = enc.encode('bolthole/v1');
export const PROTO_MUX = 'mux/1';
export const MAX_RECORD = 65535 - 16;
const EMPTY = new Uint8Array(0);

async function sha256(...parts) {
  return new Uint8Array(await subtle.digest('SHA-256', concat(...parts)));
}

async function hmac(key, data) {
  const k = await subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return new Uint8Array(await subtle.sign('HMAC', k, data));
}

// Noise HKDF: returns n 32-byte outputs.
async function hkdf(ck, ikm, n) {
  const temp = await hmac(ck, ikm);
  const out = [];
  let prev = EMPTY;
  for (let i = 1; i <= n; i++) {
    prev = await hmac(temp, concat(prev, new Uint8Array([i])));
    out.push(prev);
  }
  return out;
}

function nonce(n) {
  const b = new Uint8Array(12);
  new DataView(b.buffer).setBigUint64(4, BigInt(n));
  return b;
}

/** Noise CipherState for AES-256-GCM. */
export class CipherState {
  constructor(key) {
    this.n = 0;
    this.keyBytes = key;
    this.key = key ? subtle.importKey('raw', key, 'AES-GCM', false, ['encrypt', 'decrypt']) : null;
  }
  hasKey() {
    return this.key !== null;
  }
  async encrypt(ad, pt) {
    if (!this.key) return pt;
    const n = this.n++;
    const k = await this.key;
    return new Uint8Array(await subtle.encrypt({ name: 'AES-GCM', iv: nonce(n), additionalData: ad }, k, pt));
  }
  async decrypt(ad, ct) {
    if (!this.key) return ct;
    const n = this.n++;
    const k = await this.key;
    return new Uint8Array(await subtle.decrypt({ name: 'AES-GCM', iv: nonce(n), additionalData: ad }, k, ct));
  }
}

class SymmetricState {
  static async init(prologue) {
    const s = new SymmetricState();
    s.h = PROTOCOL.length <= 32 ? concat(PROTOCOL, new Uint8Array(32 - PROTOCOL.length)) : await sha256(PROTOCOL);
    s.ck = s.h;
    s.cs = new CipherState(null);
    await s.mixHash(prologue);
    return s;
  }
  async mixHash(data) {
    this.h = await sha256(this.h, data);
  }
  async mixKey(ikm) {
    const [ck, k] = await hkdf(this.ck, ikm, 2);
    this.ck = ck;
    this.cs = new CipherState(k);
  }
  async mixKeyAndHash(ikm) {
    const [ck, th, k] = await hkdf(this.ck, ikm, 3);
    this.ck = ck;
    await this.mixHash(th);
    this.cs = new CipherState(k);
  }
  async encryptAndHash(pt) {
    const ct = await this.cs.encrypt(this.h, pt);
    await this.mixHash(ct);
    return ct;
  }
  async decryptAndHash(ct) {
    const pt = await this.cs.decrypt(this.h, ct);
    await this.mixHash(ct);
    return pt;
  }
  async split() {
    const [k1, k2] = await hkdf(this.ck, EMPTY, 2);
    return [new CipherState(k1), new CipherState(k2)];
  }
}

async function generateX25519(fixedPriv) {
  if (fixedPriv) {
    // Tests only: import a fixed private key via PKCS#8.
    const pkcs8 = concat(new Uint8Array([0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x04, 0x22, 0x04, 0x20]), fixedPriv);
    const priv = await subtle.importKey('pkcs8', pkcs8, { name: 'X25519' }, true, ['deriveBits']);
    const jwk = await subtle.exportKey('jwk', priv);
    const pub = await subtle.importKey('jwk', { kty: 'OKP', crv: 'X25519', x: jwk.x }, { name: 'X25519' }, true, []);
    return { privateKey: priv, pub: new Uint8Array(await subtle.exportKey('raw', pub)) };
  }
  const kp = await subtle.generateKey({ name: 'X25519' }, true, ['deriveBits']);
  return { privateKey: kp.privateKey, pub: new Uint8Array(await subtle.exportKey('raw', kp.publicKey)) };
}

async function dh(privateKey, peerPub) {
  const pub = await subtle.importKey('raw', peerPub, { name: 'X25519' }, false, []);
  return new Uint8Array(await subtle.deriveBits({ name: 'X25519', public: pub }, privateKey, 256));
}

const HANDSHAKE_FAILED = 'bolthole: e2e handshake failed';

/**
 * Initiator. `conn` has read(): Promise<Uint8Array> and write(bytes).
 * @returns {Promise<{channel: Channel, hello: object}>}
 */
export async function clientHandshake(conn, psk, hello = {}, opts = {}) {
  const ss = await SymmetricState.init(PROLOGUE);
  const payload = enc.encode(JSON.stringify({ protos: [PROTO_MUX], ...hello }));
  const offered = JSON.parse(dec.decode(payload)).protos;
  await ss.mixKeyAndHash(psk);
  const e = await generateX25519(opts.ephemeral);
  await ss.mixHash(e.pub);
  await ss.mixKey(e.pub);
  const msg1 = concat(e.pub, await ss.encryptAndHash(payload));
  await conn.write(msg1);

  const msg2 = await conn.read();
  if (msg2.length < 32 + 16) throw new Error(HANDSHAKE_FAILED);
  const re = msg2.subarray(0, 32);
  let agentHello;
  try {
    await ss.mixHash(re);
    await ss.mixKey(re);
    await ss.mixKey(await dh(e.privateKey, re));
    agentHello = JSON.parse(dec.decode(await ss.decryptAndHash(msg2.subarray(32))));
  } catch {
    throw new Error(HANDSHAKE_FAILED);
  }
  if (!offered.includes(agentHello.proto)) throw new Error(HANDSHAKE_FAILED);
  const [send, recv] = await ss.split();
  return { channel: new Channel(conn, send, recv), hello: agentHello };
}

/**
 * Responder. `choose(clientHello)` returns the agent hello (with `proto`)
 * or throws to refuse.
 */
export async function agentHandshake(conn, psk, choose, opts = {}) {
  const ss = await SymmetricState.init(PROLOGUE);
  const msg1 = await conn.read();
  if (msg1.length < 32 + 16) throw new Error(HANDSHAKE_FAILED);
  const re = msg1.subarray(0, 32);
  let clientHello;
  try {
    await ss.mixKeyAndHash(psk);
    await ss.mixHash(re);
    await ss.mixKey(re);
    clientHello = JSON.parse(dec.decode(await ss.decryptAndHash(msg1.subarray(32))));
  } catch {
    throw new Error(HANDSHAKE_FAILED);
  }
  const reply = await choose(clientHello);
  const e = await generateX25519(opts.ephemeral);
  await ss.mixHash(e.pub);
  await ss.mixKey(e.pub);
  await ss.mixKey(await dh(e.privateKey, re));
  const msg2 = concat(e.pub, await ss.encryptAndHash(enc.encode(JSON.stringify(reply))));
  await conn.write(msg2);
  const [recv, send] = await ss.split();
  return { channel: new Channel(conn, send, recv), hello: clientHello };
}

/**
 * The encrypted message channel. Same shape as its transport:
 * read(): Promise<Uint8Array>, write(bytes): Promise<void>, close().
 * Writes are encrypted in call order even though WebCrypto is async.
 */
export class Channel {
  constructor(conn, send, recv) {
    this.conn = conn;
    this.send = send;
    this.recv = recv;
    this.tail = Promise.resolve();
    this.readTail = Promise.resolve();
  }
  write(msg) {
    if (msg.length > MAX_RECORD) return Promise.reject(new Error(`bolthole: record of ${msg.length} bytes is too large`));
    const ctP = this.send.encrypt(EMPTY, msg); // nonce is taken synchronously
    const p = this.tail.then(async () => this.conn.write(await ctP));
    this.tail = p.catch(() => {});
    return p;
  }
  read() {
    const p = this.readTail.then(async () => {
      const ct = await this.conn.read();
      try {
        return await this.recv.decrypt(EMPTY, ct);
      } catch {
        this.conn.close();
        throw new Error('bolthole: record failed authentication');
      }
    });
    this.readTail = p.catch(() => {});
    return p;
  }
  close() {
    this.conn.close();
  }
}

export { Queue };
