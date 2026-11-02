// Session-level flow control, over an in-memory transport.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Session, INITIAL_WINDOW, SESSION_WINDOW } from '../src/mux.js';
import { deferred } from '../src/bytes.js';

function pipe() {
  const mk = () => ({ q: [], wait: null });
  const a = mk(), b = mk();
  const end = (rx, tx) => ({
    async read() {
      while (!rx.q.length) await (rx.wait = deferred()).promise;
      return rx.q.shift();
    },
    write(m) {
      tx.q.push(m.slice());
      tx.wait?.resolve();
    },
    close() {},
  });
  return [end(a, b), end(b, a)];
}

test('streams share one session window and resume once the peer reads', async () => {
  const [ca, cb] = pipe();
  const ins = [];
  const cli = new Session(ca, { dialer: true, heartbeatMs: 0 });
  const srv = new Session(cb, { dialer: false, heartbeatMs: 0, onStream: (s) => ins.push(s) });
  const n = 24; // 24 × 4 MiB per-stream windows = 96 MiB > SESSION_WINDOW
  const data = new Uint8Array(INITIAL_WINDOW);
  const outs = Array.from({ length: n }, () => cli.open('sink'));
  const writes = outs.map((st) => st.write(data).then(() => st.closeWrite()));

  await new Promise((r) => setTimeout(r, 300));
  const buffered = ins.reduce((sum, st) => sum + st.bufLen, 0);
  assert.equal(buffered, SESSION_WINDOW);

  const reads = ins.map((st) => st.readAll());
  await Promise.all(writes);
  for (const b of await Promise.all(reads)) assert.equal(b.length, data.length);
  cli.close();
  srv.close();
});
