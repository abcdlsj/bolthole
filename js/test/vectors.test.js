// Checks the JS implementation byte for byte against spec/test-vectors.json,
// which the Go implementation generates.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { derive, parseConnString, formatConnString } from '../src/codes.js';
import { clientHandshake, agentHandshake } from '../src/noise.js';
import { encodeFrame, decodeFrame } from '../src/mux.js';
import { hex, enc, dec } from '../src/bytes.js';

const V = JSON.parse(readFileSync(new URL('../../spec/test-vectors.json', import.meta.url)));
const unhex = (s) => new Uint8Array(s.match(/../g)?.map((h) => parseInt(h, 16)) ?? []);

test('derivation', async () => {
  const k = await derive(V.code);
  assert.equal(k.routingToken, V.routing_token);
  assert.equal(k.tokenHash, V.token_hash);
  assert.equal(hex(k.psk), V.psk);
  assert.equal(formatConnString('https://relay.example.com', V.code), V.conn_string);
  const cs = parseConnString(V.conn_string);
  assert.equal(cs.relay, 'https://relay.example.com');
});

test('frames', () => {
  for (const f of V.frames) {
    assert.equal(hex(encodeFrame(f.type, f.stream_id, unhex(f.payload_hex))), f.hex);
    const d = decodeFrame(unhex(f.hex));
    assert.equal(d.type, f.type);
    assert.equal(d.id, f.stream_id);
  }
});

function scripted(reads) {
  const writes = [];
  return { writes, read: async () => reads.shift(), write: async (b) => void writes.push(b), close() {} };
}

test('handshake (client side)', async () => {
  const H = V.handshake;
  const { psk } = await derive(V.code);
  const conn = scripted([unhex(H.msg2)]);
  const { channel, hello } = await clientHandshake(conn, psk, JSON.parse(H.client_hello), { ephemeral: unhex(H.client_ephemeral_private) });
  assert.equal(hex(conn.writes[0]), H.msg1);
  assert.equal(hello.name, 'vector-agent');
  await channel.write(enc.encode('ping'));
  assert.equal(hex(conn.writes[1]), H.client_record_0);
});

test('handshake (agent side)', async () => {
  const H = V.handshake;
  const { psk } = await derive(V.code);
  const conn = scripted([unhex(H.msg1), unhex(H.client_record_0)]);
  const { channel, hello } = await agentHandshake(conn, psk, () => JSON.parse(H.agent_hello), { ephemeral: unhex(H.agent_ephemeral_private) });
  assert.equal(hello.name, 'vector-client');
  assert.equal(hex(conn.writes[0]), H.msg2);
  assert.equal(dec.decode(await channel.read()), 'ping');
  await channel.write(enc.encode('pong'));
  assert.equal(hex(conn.writes[1]), H.agent_record_0);
});

test('wrong psk is rejected', async () => {
  const H = V.handshake;
  const { psk } = await derive(V.code);
  psk[0] ^= 1;
  const conn = scripted([unhex(H.msg2)]);
  await assert.rejects(clientHandshake(conn, psk, JSON.parse(H.client_hello), { ephemeral: unhex(H.client_ephemeral_private) }));
});
