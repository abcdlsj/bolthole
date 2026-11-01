// Connect codes, routing tokens and connection strings (spec §1).
import { b64url, unb64url, hex, enc } from './bytes.js';

const SALT = enc.encode('bolthole/v1');
const subtle = globalThis.crypto.subtle;

/** A fresh random 32-byte connect code, base64url encoded. */
export function newCode() {
  return b64url(crypto.getRandomValues(new Uint8Array(32)));
}

export function parseCode(s) {
  const b = unb64url(String(s).trim());
  if (b.length !== 32) throw new Error('bolthole: a connect code is 43 base64url characters');
  return b;
}

async function hkdf(ikm, info, len) {
  const key = await subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
  const bits = await subtle.deriveBits({ name: 'HKDF', hash: 'SHA-256', salt: SALT, info: enc.encode(info) }, key, len * 8);
  return new Uint8Array(bits);
}

/** SHA-256 of the routing token, lowercase hex. */
export async function hashToken(token) {
  return hex(new Uint8Array(await subtle.digest('SHA-256', enc.encode(token))));
}

/**
 * Everything derived from a code.
 * @param {Uint8Array|string} code
 * @returns {Promise<{routingToken: string, tokenHash: string, psk: Uint8Array}>}
 */
export async function derive(code) {
  const c = typeof code === 'string' ? parseCode(code) : code;
  const routingToken = b64url(await hkdf(c, 'routing-token', 24));
  const psk = await hkdf(c, 'e2e-psk', 32);
  return { routingToken, tokenHash: await hashToken(routingToken), psk };
}

/** "https://relay/#k=<code>" */
export function formatConnString(relay, code) {
  const u = new URL(relay);
  if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error('bolthole: relay URL must be http(s)');
  u.hash = '';
  if (!u.pathname.endsWith('/')) u.pathname += '/';
  return `${u.toString()}#k=${typeof code === 'string' ? code : b64url(code)}`;
}

/**
 * Parses "https://relay/#k=<code>[&name=…]".
 * @returns {{relay: string, code: Uint8Array, name: string}} relay has no trailing slash.
 */
export function parseConnString(s) {
  const u = new URL(String(s).trim());
  if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error('bolthole: connection string must start with http(s)://');
  const frag = new URLSearchParams(u.hash.replace(/^#/, ''));
  const code = parseCode(frag.get('k') || '');
  u.hash = '';
  u.search = '';
  const relay = u.toString().replace(/\/+$/, '');
  return { relay, code, name: frag.get('name') || '' };
}
