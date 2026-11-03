// tests/unit/api_session.test.js
// Unit tests for the Actual API session coordinator (src/lib/api-session.ts).
// Uses injected fake init/shutdown/sync hooks — no Actual server needed.
//
// Covers the "server hangs under concurrent use" fixes:
//   - a write queued while a batch is running is never stranded
//   - a steady stream of writes cannot postpone processing forever
//   - hung init / operation / shutdown release the lock (bounded hold time)
//   - lock-wait timeout fails fast and leaves the queue healthy
//   - nested calls inside an open session don't deadlock
//
// Run via: npm run test:unit-js   (after npm run build)
// Or:      node tests/unit/api_session.test.js

let failures = 0;
const pass = (label) => console.log(`  ✓ ${label}`);
const fail = (label, d = '') => { console.error(`  ✗ FAIL: ${label}${d ? ' — ' + d : ''}`); failures++; };
const check = (cond, label, d = '') => (cond ? pass(label) : fail(label, d));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const never = () => new Promise(() => {});

/** Callers resolve before the batch's sync/shutdown; wait for those to finish. */
async function waitIdle(mgr, ms = 1000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const st = mgr.getState();
    if (!st.lockHeld && !st.writeDrainActive && st.pendingWrites === 0) return true;
    await sleep(5);
  }
  return false;
}

/** Resolves to 'done' | 'rejected:<msg>' | 'pending' after `ms`. */
async function settleWithin(p, ms) {
  return Promise.race([
    p.then(() => 'done', (e) => `rejected:${e?.message ?? e}`),
    sleep(ms).then(() => 'pending'),
  ]);
}

(async () => {
  const { createApiSessionManager, ApiTimeoutError, withTimeout, API_ERROR_CODES } = await import('../../dist/src/lib/api-session.js');

  /** Manager with counting fake hooks; `overrides` replaces individual hooks/options. */
  function makeManager(overrides = {}) {
    const stats = { init: 0, shutdown: 0, sync: 0, open: 0, maxOpen: 0 };
    const mgr = createApiSessionManager({
      init: async () => {
        stats.init++; stats.open++; stats.maxOpen = Math.max(stats.maxOpen, stats.open);
        await sleep(5);
      },
      shutdown: async () => { stats.shutdown++; stats.open--; await sleep(5); },
      sync: async () => { stats.sync++; await sleep(5); },
      writeBatchDelayMs: 20,
      timeouts: { lockWaitMs: 2000, initMs: 2000, operationMs: 2000, syncMs: 2000, shutdownMs: 2000 },
      ...overrides,
    });
    return { mgr, stats };
  }

  // ── 1. Regression: write queued mid-batch must not be stranded ─────────────
  console.log('\n[1] Write queued while a batch is running completes');
  {
    const { mgr, stats } = makeManager();
    const a = mgr.queueWrite(async () => { await sleep(150); return 'A'; });
    await sleep(60); // A's batch is now running; B's batch timer will fire mid-batch
    const b = mgr.queueWrite(async () => 'B');
    check((await settleWithin(a, 1000)) === 'done', 'first write completes');
    check((await settleWithin(b, 1000)) === 'done', 'second write (queued mid-batch) completes — no stranding');
    check(await b === 'B', 'second write returns its result');
    await waitIdle(mgr);
    check(stats.init === 2 && stats.shutdown === 2, 'two batches → two sessions', JSON.stringify(stats));
    check(stats.maxOpen === 1, 'sessions never overlap');
  }

  // ── 2. Steady write stream cannot postpone processing forever ──────────────
  console.log('\n[2] Steady stream of writes is processed (fixed window, not debounce)');
  {
    const { mgr } = makeManager({ writeBatchDelayMs: 50 });
    const first = mgr.queueWrite(async () => 'first');
    let stop = false;
    (async () => { while (!stop) { mgr.queueWrite(async () => 'x').catch(() => {}); await sleep(10); } })();
    const r = await settleWithin(first, 400);
    stop = true;
    check(r === 'done', 'first write completes while new writes keep arriving every 10ms', r);
    await sleep(200);
  }

  // ── 3. Batching: concurrent writes share one session + one sync ────────────
  console.log('\n[3] Writes arriving together share one session, run sequentially');
  {
    const { mgr, stats } = makeManager();
    let concurrent = 0, maxConcurrent = 0;
    const op = (v) => async () => { concurrent++; maxConcurrent = Math.max(maxConcurrent, concurrent); await sleep(10); concurrent--; return v; };
    const results = await Promise.all([mgr.queueWrite(op(1)), mgr.queueWrite(op(2)), mgr.queueWrite(op(3))]);
    check(JSON.stringify(results) === '[1,2,3]', 'all writes resolve with their own results');
    await waitIdle(mgr);
    check(stats.init === 1 && stats.sync === 1 && stats.shutdown === 1, 'one init/sync/shutdown for the batch', JSON.stringify(stats));
    check(maxConcurrent === 1, 'writes within a batch run one at a time');
  }

  // ── 4. One failing write doesn't fail the rest of the batch ────────────────
  console.log('\n[4] Write errors are isolated per operation');
  {
    const { mgr, stats } = makeManager();
    const ok1 = mgr.queueWrite(async () => 'ok1');
    const bad = mgr.queueWrite(async () => { throw new Error('boom'); });
    const ok2 = mgr.queueWrite(async () => 'ok2');
    check(await ok1 === 'ok1' && await ok2 === 'ok2', 'other writes succeed');
    check((await settleWithin(bad, 100)) === 'rejected:boom', 'failing write rejects with its error');
    await waitIdle(mgr);
    check(stats.sync === 1 && stats.shutdown === 1, 'batch still synced and shut down');
  }

  // ── 5. Hung operation releases the lock ────────────────────────────────────
  console.log('\n[5] Hung read operation times out and releases the session');
  {
    const { mgr, stats } = makeManager({ timeouts: { operationMs: 100, lockWaitMs: 2000 } });
    const hung = mgr.withSession(never);
    const next = mgr.withSession(async () => 'next');
    const r = await settleWithin(hung, 500);
    check(r.startsWith('rejected:') && r.includes('timed out'), 'hung operation rejects with a timeout error', r);
    check((await settleWithin(next, 500)) === 'done', 'queued operation runs after the timeout');
    check(stats.shutdown === 2, 'shutdown ran for both sessions', JSON.stringify(stats));
    const err = await hung.catch((e) => e);
    check(err instanceof ApiTimeoutError && err.phase === 'operation', 'error is ApiTimeoutError(phase=operation)');
    check(err.code === API_ERROR_CODES.UPSTREAM_TIMEOUT && err.code === -32002, 'operation timeout → JSON-RPC code -32002');
    check(err.data && err.data.retryable === false, 'operation timeout is not marked retryable (write may have applied)');
  }

  // ── 6. Hung write in a batch: rest of the batch still completes ────────────
  console.log('\n[6] Hung write times out without blocking its batch');
  {
    const { mgr, stats } = makeManager({ timeouts: { operationMs: 100 } });
    const hung = mgr.queueWrite(never);
    const after = mgr.queueWrite(async () => 'after');
    check((await settleWithin(hung, 500)).includes('timed out'), 'hung write rejects with timeout');
    check((await settleWithin(after, 500)) === 'done', 'next write in the same batch completes');
    await waitIdle(mgr);
    check(stats.sync === 1 && stats.shutdown === 1, 'batch synced and shut down');
  }

  // ── 7. Hung init: callers fail, server recovers ────────────────────────────
  console.log('\n[7] Hung init (unreachable Actual server) times out');
  {
    let hangInit = true;
    const { mgr } = makeManager({
      init: async () => { if (hangInit) await never(); },
      timeouts: { initMs: 100 },
    });
    const r1 = await settleWithin(mgr.withSession(async () => 'read'), 500);
    check(r1.startsWith('rejected:') && r1.includes('ACTUAL_SERVER_URL'), 'read rejects with an init timeout hint', r1);
    const w1 = await settleWithin(mgr.queueWrite(async () => 'write'), 500);
    check(w1.startsWith('rejected:'), 'queued write rejects too', w1);
    hangInit = false;
    check((await settleWithin(mgr.withSession(async () => 'ok'), 500)) === 'done', 'next call succeeds once the server recovers');
  }

  // ── 8. Hung shutdown still releases the lock ───────────────────────────────
  console.log('\n[8] Hung shutdown does not keep the lock');
  {
    const { mgr } = makeManager({ shutdown: never, timeouts: { shutdownMs: 100 } });
    const a = mgr.withSession(async () => 'a');
    const b = mgr.withSession(async () => 'b');
    check((await settleWithin(a, 500)) === 'done', 'first call resolves despite hung shutdown');
    check((await settleWithin(b, 500)) === 'done', 'second call gets the lock after shutdown timeout');
  }

  // ── 9. Lock-wait timeout fails fast, queue stays healthy ───────────────────
  console.log('\n[9] Lock-wait timeout');
  {
    const { mgr } = makeManager({ timeouts: { lockWaitMs: 80, operationMs: 2000 } });
    const holder = mgr.withSession(async () => { await sleep(250); return 'holder'; });
    await sleep(10);
    const waiter = mgr.withSession(async () => 'waiter');
    const r = await settleWithin(waiter, 500);
    check(r.startsWith('rejected:Server busy'), 'waiter rejects with "Server busy"', r);
    const e2 = await waiter.catch((x) => x);
    check(e2 instanceof ApiTimeoutError && e2.code === -32001, 'lock-wait timeout → JSON-RPC code -32001', String(e2?.code));
    check(e2?.data?.retryable === true && e2?.data?.retryAfterMs > 0, 'lock-wait error data: retryable + retryAfterMs');
    check(mgr.getState().lockWaiters === 0, 'timed-out waiter removed from the queue');
    check(await holder === 'holder', 'holder unaffected');
    check((await settleWithin(mgr.withSession(async () => 'later'), 500)) === 'done', 'later calls still acquire the lock');
  }

  // ── 10. Re-entrancy: nested calls don't deadlock ───────────────────────────
  console.log('\n[10] Nested calls run inside the open session');
  {
    const { mgr, stats } = makeManager({ timeouts: { lockWaitMs: 300 } });
    const nestedRead = await settleWithin(
      mgr.withSession(async () => mgr.withSession(async () => 'inner')), 1000);
    check(nestedRead === 'done', 'withSession inside withSession');
    const writeInRead = await settleWithin(
      mgr.withSession(async () => mgr.queueWrite(async () => 'w')), 1000);
    check(writeInRead === 'done', 'queueWrite inside withSession');
    const readInWrite = await settleWithin(
      mgr.queueWrite(async () => mgr.withSession(async () => 'r')), 1000);
    check(readInWrite === 'done', 'withSession inside a queued write');
    const exclusiveInSession = await settleWithin(
      mgr.withSession(async () => mgr.runExclusive(async () => 'x')), 1000);
    check(exclusiveInSession === 'done', 'runExclusive inside withSession');
    check(stats.init === 4, 'nested calls reuse the session (one init per outer call)', JSON.stringify(stats));
    check(mgr.inSession() === false, 'inSession() is false outside a session');
  }

  // ── 11. Callbacks that outlive their session don't bypass the lock ─────────
  console.log('\n[11] Late callback from a finished session queues normally');
  {
    const { mgr, stats } = makeManager();
    let late;
    await mgr.withSession(async () => {
      late = new Promise((resolve) => setTimeout(() => resolve(mgr.withSession(async () => mgr.inSession())), 50));
    });
    const sawSession = await late;
    check(sawSession === true, 'late call ran in a session of its own');
    check(stats.init === 2 && stats.maxOpen === 1, 'late call opened a new locked session', JSON.stringify(stats));
  }

  // ── 12. runExclusive serialises with sessions ──────────────────────────────
  console.log('\n[12] runExclusive waits for the in-flight session');
  {
    const { mgr } = makeManager();
    const order = [];
    const s = mgr.withSession(async () => { await sleep(80); order.push('session'); });
    await sleep(10);
    const x = mgr.runExclusive(async () => { order.push('exclusive'); });
    await Promise.all([s, x]);
    check(order.join(',') === 'session,exclusive', 'exclusive ran after the session', order.join(','));
  }

  // ── 13. Load: many concurrent mixed calls all settle, never overlap ────────
  console.log('\n[13] 60 concurrent mixed reads/writes');
  {
    const { mgr, stats } = makeManager({ writeBatchDelayMs: 5 });
    const calls = [];
    for (let i = 0; i < 60; i++) {
      calls.push(i % 3 === 0
        ? mgr.queueWrite(async () => { await sleep(Math.random() * 5); return i; })
        : mgr.withSession(async () => { await sleep(Math.random() * 5); return i; }));
    }
    const r = await settleWithin(Promise.all(calls), 5000);
    check(r === 'done', 'all 60 calls settle', r);
    check(stats.maxOpen === 1, 'sessions never overlapped', JSON.stringify(stats));
    check(await waitIdle(mgr), 'coordinator drains to idle');
    const st = mgr.getState();
    check(!st.lockHeld && st.lockWaiters === 0 && st.pendingWrites === 0, 'coordinator idle afterwards', JSON.stringify(st));
  }

  // ── 14. withTimeout helper ─────────────────────────────────────────────────
  console.log('\n[14] withTimeout');
  {
    check(await withTimeout(Promise.resolve(7), 50, 'operation') === 7, 'passes through a fast result');
    const t = await settleWithin(withTimeout(never(), 50, 'operation'), 500);
    check(t.startsWith('rejected:') && t.includes('timed out after 50ms'), 'rejects a slow promise after the timeout', t);
    const disabled = await settleWithin(withTimeout(never(), 0, 'operation'), 100);
    check(disabled === 'pending', 'timeout of 0 disables the timer');
    let unhandled = false;
    const onUnhandled = () => { unhandled = true; };
    process.on('unhandledRejection', onUnhandled);
    await withTimeout(sleep(80).then(() => { throw new Error('late'); }), 20, 'operation').catch(() => {});
    await sleep(120);
    process.off('unhandledRejection', onUnhandled);
    check(!unhandled, 'late rejection of an abandoned operation is not unhandled');
  }

  // ══ Keep-alive mode ════════════════════════════════════════════════════════
  /** Keep-alive manager with controllable sync failures and session key. */
  function makeKeepAlive(overrides = {}) {
    const stats = { init: 0, shutdown: 0, sync: 0, open: 0, maxOpen: 0 };
    const ctl = { failSync: false, key: 'budget-a' };
    const mgr = createApiSessionManager({
      init: async () => { stats.init++; stats.open++; stats.maxOpen = Math.max(stats.maxOpen, stats.open); await sleep(5); },
      shutdown: async () => { stats.shutdown++; stats.open = Math.max(0, stats.open - 1); await sleep(5); },
      sync: async () => { stats.sync++; await sleep(2); if (ctl.failSync) throw new Error('sync failed'); },
      sessionKey: () => ctl.key,
      keepAliveMs: 150,
      syncFreshnessMs: 0,
      writeBatchDelayMs: 10,
      timeouts: { lockWaitMs: 2000, initMs: 2000, operationMs: 2000, syncMs: 2000, shutdownMs: 2000 },
      ...overrides,
    });
    return { mgr, stats, ctl };
  }

  console.log('\n[K1] Keep-alive: reads reuse one open session, refresh with sync, close when idle');
  {
    const { mgr, stats } = makeKeepAlive();
    await mgr.withSession(async () => 1);
    await mgr.withSession(async () => 2);
    await mgr.withSession(async () => 3);
    check(stats.init === 1, 'init (login + load) only once for 3 calls', JSON.stringify(stats));
    check(stats.sync === 2, 'calls 2 and 3 refresh with sync()', JSON.stringify(stats));
    check(stats.shutdown === 0 && mgr.getState().sessionOpen, 'session stays open between calls');
    await sleep(300);
    check(stats.shutdown === 1 && !mgr.getState().sessionOpen, 'session closed after keepAliveMs idle', JSON.stringify(stats));
    await mgr.withSession(async () => 4);
    check(stats.init === 2, 'next call after idle close reopens');
    await mgr.close();
  }

  console.log('\n[K2] Keep-alive: refresh skipped inside the freshness window');
  {
    const { mgr, stats } = makeKeepAlive({ syncFreshnessMs: 10_000 });
    await mgr.withSession(async () => 1);
    await mgr.withSession(async () => 2);
    check(stats.init === 1 && stats.sync === 0, 'second call within freshness window does no sync', JSON.stringify(stats));
    await mgr.close();
  }

  console.log('\n[K3] Keep-alive: writes are synced (pushed) but the session stays open');
  {
    const { mgr, stats } = makeKeepAlive();
    await Promise.all([mgr.queueWrite(async () => 'a'), mgr.queueWrite(async () => 'b')]);
    await waitIdle(mgr);
    check(stats.init === 1 && stats.shutdown === 0, 'one open, no shutdown', JSON.stringify(stats));
    check(stats.sync === 1, 'one post-batch sync', JSON.stringify(stats));
    await mgr.withSession(async () => 'read');
    check(stats.init === 1, 'subsequent read reuses the session');
    await mgr.close();
    check(stats.shutdown === 1 && !mgr.getState().sessionOpen, 'close() shuts the session down');
  }

  console.log('\n[K4] Keep-alive: syncAfter forces a sync after a read-path mutation');
  {
    const { mgr, stats } = makeKeepAlive({ syncFreshnessMs: 10_000 });
    await mgr.withSession(async () => 'bank sync', { syncAfter: true });
    check(stats.sync === 1, 'sync ran after the operation', JSON.stringify(stats));
    await mgr.close();
  }

  console.log('\n[K5] Keep-alive: budget switch reopens on the new budget');
  {
    const { mgr, stats, ctl } = makeKeepAlive();
    await mgr.withSession(async () => 1);
    ctl.key = 'budget-b';
    await mgr.withSession(async () => 2);
    check(stats.init === 2 && stats.shutdown === 1, 'old session closed, new one opened', JSON.stringify(stats));
    check(stats.maxOpen === 1, 'never two sessions open');
    await mgr.close();
  }

  console.log('\n[K6] Keep-alive: failed refresh sync (expired token, server restart) reopens');
  {
    const { mgr, stats, ctl } = makeKeepAlive();
    await mgr.withSession(async () => 1);
    ctl.failSync = true;
    const r = await settleWithin(mgr.withSession(async () => 'after'), 1000);
    check(r === 'done', 'call still succeeds', r);
    check(stats.init === 2 && stats.shutdown === 1, 'session was reopened', JSON.stringify(stats));
    ctl.failSync = false;
    await mgr.close();
  }

  console.log('\n[K7] Keep-alive: failed post-write sync closes the session (shutdown pushes)');
  {
    const { mgr, stats, ctl } = makeKeepAlive();
    await mgr.withSession(async () => 1);
    ctl.failSync = true;
    const w = await settleWithin(mgr.queueWrite(async () => 'w'), 1000);
    await waitIdle(mgr);
    check(w === 'done', 'write itself still resolves', w);
    check(!mgr.getState().sessionOpen && stats.shutdown >= 1, 'session closed after failed sync', JSON.stringify(stats));
    ctl.failSync = false;
  }

  console.log('\n[K8] Keep-alive: timeout closes the session; next call opens fresh');
  {
    const { mgr, stats } = makeKeepAlive({ timeouts: { operationMs: 80 } });
    await mgr.withSession(async () => 1);
    const r = await settleWithin(mgr.withSession(never), 500);
    check(r.includes('timed out'), 'hung call times out', r);
    check(!mgr.getState().sessionOpen && stats.shutdown === 1, 'session closed after timeout', JSON.stringify(stats));
    await mgr.withSession(async () => 2);
    check(stats.init === 2, 'next call reopened');
    await mgr.close();
  }

  console.log('\n[K9] Keep-alive: ordinary error keeps the session but forces a health-check sync');
  {
    const { mgr, stats } = makeKeepAlive({ syncFreshnessMs: 10_000 });
    await mgr.withSession(async () => 1);
    await mgr.withSession(async () => { throw new Error('account not found'); }).catch(() => {});
    check(mgr.getState().sessionOpen && stats.shutdown === 0, 'session still open after a normal error');
    await mgr.withSession(async () => 2);
    check(stats.sync === 1, 'next call syncs despite freshness window', JSON.stringify(stats));
    await mgr.close();
  }

  console.log('\n[K10] Keep-alive: max session age forces a full reopen');
  {
    const { mgr, stats } = makeKeepAlive({ maxSessionAgeMs: 50 });
    await mgr.withSession(async () => 1);
    await sleep(80);
    await mgr.withSession(async () => 2);
    check(stats.init === 2 && stats.shutdown === 1, 'reopened after max age', JSON.stringify(stats));
    await mgr.close();
  }

  console.log('\n[K11] Keep-alive: close() during a call defers until the call ends');
  {
    const { mgr, stats } = makeKeepAlive();
    await mgr.withSession(async () => { await mgr.close(); check(stats.shutdown === 0, 'not closed mid-call'); });
    check(stats.shutdown === 1 && !mgr.getState().sessionOpen, 'closed once the call finished');
  }

  console.log('\n[K12] Keep-alive: 60 concurrent mixed calls, one session, no overlap');
  {
    const { mgr, stats } = makeKeepAlive({ writeBatchDelayMs: 5 });
    const calls = [];
    for (let i = 0; i < 60; i++) {
      calls.push(i % 3 === 0
        ? mgr.queueWrite(async () => { await sleep(Math.random() * 3); return i; })
        : mgr.withSession(async () => { await sleep(Math.random() * 3); return i; }));
    }
    check((await settleWithin(Promise.all(calls), 5000)) === 'done', 'all calls settle');
    await waitIdle(mgr);
    check(stats.init === 1 && stats.maxOpen === 1, 'single open session for the whole burst', JSON.stringify(stats));
    await mgr.close();
  }

  console.log('\n[K13] keepAliveMs without a sync hook falls back to cold mode');
  {
    const { mgr, stats } = makeManager({ sync: undefined, keepAliveMs: 1000 });
    await mgr.withSession(async () => 1);
    check(!mgr.getState().keepAlive && stats.shutdown === 1, 'closed after the call', JSON.stringify(stats));
  }

  console.log(failures === 0 ? '\n✅ api_session tests passed' : `\n❌ ${failures} api_session test(s) failed`);
  process.exit(failures === 0 ? 0 : 1);
})().catch((e) => {
  console.error('api_session tests crashed:', e);
  process.exit(2);
});
