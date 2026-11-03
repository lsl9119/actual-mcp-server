/**
 * Actual API session coordinator.
 *
 * `@actual-app/api` is a process-wide singleton backed by a single SQLite
 * connection: only one init → work → shutdown cycle may be in flight at a time.
 * This module owns that invariant and guarantees the server can never wedge
 * permanently, whatever the Actual server or SDK does:
 *
 *  - **FIFO mutex with a wait timeout** — callers that cannot get the session
 *    within `lockWaitMs` fail fast ("server busy") instead of queueing forever.
 *  - **Bounded hold time** — init/download, the operation, sync and shutdown
 *    each have their own timeout, so a hung SDK call releases the lock.
 *  - **Re-entrancy** — a call made from inside an active session (e.g. a write
 *    operation that calls a read helper) runs inline in that session instead of
 *    deadlocking on the lock it already holds.
 *  - **Write batching without stranding** — writes that arrive within
 *    `writeBatchDelayMs` share one session; a single drain loop guarantees every
 *    queued write is processed, even when it arrives mid-batch.
 *  - **Optional keep-alive** (`keepAliveMs > 0`) — instead of init (which logs
 *    in to the Actual server) + load + shutdown around every call, the budget
 *    stays open between calls. Each call first runs `sync()` to pull remote
 *    changes, writes are followed by `sync()` to push them, and the session is
 *    closed (shutdown → sync + close) after `keepAliveMs` of inactivity, after
 *    `maxSessionAgeMs`, when `sessionKey()` changes (budget switch), or after
 *    any timeout/failed sync. Persistence is the same as cold mode: changes
 *    reach the server through `sync()` either way.
 *
 * The module is dependency-free (hooks are injected) so it can be unit tested
 * without an Actual Budget server.
 */
import { AsyncLocalStorage } from 'node:async_hooks';

export interface ApiSessionTimeouts {
  /** Max time to wait for the session lock. */
  lockWaitMs: number;
  /** Max time for api.init() + downloadBudget(). */
  initMs: number;
  /** Max time for a single operation (one read, or one write in a batch). */
  operationMs: number;
  /** Max time for an api.sync(). */
  syncMs: number;
  /** Max time for api.shutdown(). */
  shutdownMs: number;
}

export const DEFAULT_API_SESSION_TIMEOUTS: ApiSessionTimeouts = {
  lockWaitMs: 30_000,
  initMs: 60_000,
  operationMs: 120_000,
  syncMs: 60_000,
  shutdownMs: 30_000,
};

/** JSON-RPC error codes (implementation-defined server error range). */
export const API_ERROR_CODES = {
  /** Session lock busy — nothing ran, always safe to retry. */
  SERVER_BUSY: -32001,
  /** Actual server / SDK did not respond in time. */
  UPSTREAM_TIMEOUT: -32002,
} as const;

interface SessionLogger {
  debug: (msg: string, ...args: unknown[]) => void;
  warn: (msg: string, ...args: unknown[]) => void;
  error: (msg: string, ...args: unknown[]) => void;
}

const noopLogger: SessionLogger = { debug: () => {}, warn: () => {}, error: () => {} };

export interface ApiSessionOptions {
  /** Opens the API session (api.init + downloadBudget). */
  init: () => Promise<void>;
  /** Closes the API session (api.shutdown). Must persist local changes. */
  shutdown: () => Promise<void>;
  /** Exchanges changes with the server (api.sync). Required for keep-alive. */
  sync?: () => Promise<void>;
  /** Timeouts in ms; 0 or a negative value disables that timeout. */
  timeouts?: Partial<ApiSessionTimeouts>;
  /** How long the first queued write waits for others to join its batch. */
  writeBatchDelayMs?: number;
  /** Keep the session open this long after the last call (0 = close after every call). */
  keepAliveMs?: number;
  /** Fully reopen a kept-alive session once it is this old (0 = no limit). */
  maxSessionAgeMs?: number;
  /** Skip the pre-call refresh sync if the last sync was this recent. */
  syncFreshnessMs?: number;
  /** Identifies what init() opens (e.g. the active budget); a change forces a reopen. */
  sessionKey?: () => string;
  logger?: SessionLogger;
}

export interface SessionCallOptions {
  /** Keep-alive mode: sync after this (read-path) operation because it changes data. */
  syncAfter?: boolean;
}

export type ApiTimeoutPhase = 'lock-wait' | 'init' | 'operation' | 'sync' | 'shutdown';

export class ApiTimeoutError extends Error {
  /** Numeric so the MCP SDK forwards it as the JSON-RPC error code. */
  readonly code: number;
  /** Forwarded by the MCP SDK as JSON-RPC error.data. */
  readonly data: {
    phase: ApiTimeoutPhase;
    timeoutMs: number;
    /** True when nothing was executed, so retrying cannot duplicate work. */
    retryable: boolean;
    retryAfterMs?: number;
  };
  constructor(readonly phase: ApiTimeoutPhase, readonly timeoutMs: number, message: string) {
    super(message);
    this.name = 'ApiTimeoutError';
    this.code = phase === 'lock-wait' ? API_ERROR_CODES.SERVER_BUSY : API_ERROR_CODES.UPSTREAM_TIMEOUT;
    const retryable = phase === 'lock-wait' || phase === 'init';
    this.data = {
      phase,
      timeoutMs,
      retryable,
      ...(phase === 'lock-wait' ? { retryAfterMs: 5_000 } : {}),
    };
  }
}

function timeoutMessage(phase: ApiTimeoutPhase, ms: number, extra = ''): string {
  switch (phase) {
    case 'lock-wait':
      return `Server busy: waited ${ms}ms for the Actual Budget session${extra}. ` +
        'Other requests are still running; nothing was executed, retry shortly (or raise ACTUAL_LOCK_WAIT_TIMEOUT_MS).';
    case 'init':
      return `Timed out after ${ms}ms connecting to Actual Budget / downloading the budget. ` +
        'Check that ACTUAL_SERVER_URL is reachable, then retry (or raise ACTUAL_INIT_TIMEOUT_MS).';
    case 'operation':
      return `Actual Budget operation timed out after ${ms}ms and was abandoned so other requests can proceed. ` +
        'If this was a write it may still have been applied — verify before retrying (or raise ACTUAL_OPERATION_TIMEOUT_MS).';
    case 'sync':
      return `Timed out after ${ms}ms syncing with the Actual Budget server (ACTUAL_SYNC_TIMEOUT_MS).`;
    case 'shutdown':
      return `Timed out after ${ms}ms closing the Actual Budget session (ACTUAL_SHUTDOWN_TIMEOUT_MS).`;
  }
}

/**
 * Race `work` against a timer. The underlying work cannot be cancelled; on
 * timeout it keeps running detached, but its eventual rejection is handled.
 */
export function withTimeout<T>(work: Promise<T>, ms: number, phase: ApiTimeoutPhase, extra = ''): Promise<T> {
  if (!Number.isFinite(ms) || ms <= 0) return work;
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new ApiTimeoutError(phase, ms, timeoutMessage(phase, ms, extra))), ms);
  });
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

/** Runs fn, converting a synchronous throw into a rejected promise. */
function invoke<T>(fn: () => Promise<T>): Promise<T> {
  return Promise.resolve().then(fn);
}

/** FIFO mutex whose waiters can give up after a timeout. */
class TimedMutex {
  private locked = false;
  private waiters: Array<() => void> = [];

  get isLocked(): boolean { return this.locked; }
  get waiting(): number { return this.waiters.length; }

  acquire(timeoutMs: number): Promise<() => void> {
    if (!this.locked) {
      this.locked = true;
      return Promise.resolve(this.makeRelease());
    }
    const ahead = this.waiters.length + 1;
    return new Promise((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      const grant = () => {
        if (timer) clearTimeout(timer);
        resolve(this.makeRelease());
      };
      this.waiters.push(grant);
      if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
        timer = setTimeout(() => {
          const idx = this.waiters.indexOf(grant);
          if (idx !== -1) {
            this.waiters.splice(idx, 1);
            reject(new ApiTimeoutError('lock-wait', timeoutMs,
              timeoutMessage('lock-wait', timeoutMs, ` (${ahead} request(s) were ahead in the queue)`)));
          }
        }, timeoutMs);
      }
    });
  }

  private makeRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.waiters.shift();
      if (next) next(); // hand the lock straight to the next waiter
      else this.locked = false;
    };
  }
}

interface SessionContext {
  /** True while the lock is held for this context. */
  active: boolean;
  /** Close (shutdown) the session when this call finishes, even in keep-alive mode. */
  closeAfter: boolean;
}

interface QueuedWrite {
  operation: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

export interface ApiSessionState {
  lockHeld: boolean;
  lockWaiters: number;
  pendingWrites: number;
  writeDrainActive: boolean;
  /** How long the current holder has held the lock (ms), or null if idle. */
  activeSessionMs: number | null;
  keepAlive: boolean;
  /** Keep-alive mode: whether a budget session is currently open. */
  sessionOpen: boolean;
  /** Age of the open session (ms), or null. */
  sessionAgeMs: number | null;
}

export interface ApiSessionManager {
  /** Run a (read) operation in a session (opened, or refreshed when kept alive). */
  withSession<T>(operation: () => Promise<T>, callOptions?: SessionCallOptions): Promise<T>;
  /** Queue a write; writes arriving together share one session and one sync. */
  queueWrite<T>(operation: () => Promise<T>): Promise<T>;
  /** Hold the lock without opening a session. */
  runExclusive<T>(fn: () => Promise<T>): Promise<T>;
  /** Shut the API session down (waits for the in-flight call). Safe to call when closed. */
  close(): Promise<void>;
  /** True when called from inside an active session. */
  inSession(): boolean;
  getState(): ApiSessionState;
}

export function createApiSessionManager(options: ApiSessionOptions): ApiSessionManager {
  const t: ApiSessionTimeouts = { ...DEFAULT_API_SESSION_TIMEOUTS, ...(options.timeouts ?? {}) };
  const batchDelay = options.writeBatchDelayMs ?? 100;
  const log = options.logger ?? noopLogger;
  const maxAge = options.maxSessionAgeMs ?? 30 * 60_000;
  const freshness = options.syncFreshnessMs ?? 1_000;
  let keepAliveMs = Math.max(0, options.keepAliveMs ?? 0);
  if (keepAliveMs > 0 && !options.sync) {
    log.warn('[API SESSION] keepAliveMs requires a sync hook; keep-alive disabled');
    keepAliveMs = 0;
  }
  const keepAlive = keepAliveMs > 0;

  const mutex = new TimedMutex();
  const als = new AsyncLocalStorage<SessionContext>();
  let lockHeldSince: number | null = null;

  // Session state (meaningful in keep-alive mode; in cold mode it is closed
  // again at the end of every call).
  let open = false;
  let openKey: string | null = null;
  let openedAt = 0;
  let lastSyncAt = 0;
  let lastUsedAt = 0;
  let idleTimer: NodeJS.Timeout | null = null;

  let writeQueue: QueuedWrite[] = [];
  let draining = false;
  let drainTimer: NodeJS.Timeout | null = null;

  // A context only counts while its lock is held; callbacks that outlive
  // their call (timers, abandoned promises) must queue like everyone else.
  const inSession = () => als.getStore()?.active === true;
  const currentKey = () => options.sessionKey?.() ?? '';

  async function safeShutdown(): Promise<void> {
    try {
      await withTimeout(invoke(options.shutdown), t.shutdownMs, 'shutdown');
    } catch (err) {
      log.error('[API SESSION] shutdown failed; releasing the session lock anyway:', err);
    }
  }

  /** Must be called with the lock held. */
  async function closeNow(): Promise<void> {
    open = false;
    openKey = null;
    await safeShutdown();
  }

  async function syncNow(): Promise<void> {
    await withTimeout(invoke(options.sync!), t.syncMs, 'sync');
    lastSyncAt = Date.now();
  }

  /** Must be called with the lock held. Opens, or refreshes a kept-alive session. */
  async function ensureOpen(): Promise<void> {
    const key = currentKey();
    if (open) {
      if (openKey !== key) {
        log.debug('[API SESSION] Active budget changed; reopening session');
        await closeNow();
      } else if (maxAge > 0 && Date.now() - openedAt >= maxAge) {
        log.debug('[API SESSION] Kept-alive session reached max age; reopening');
        await closeNow();
      } else {
        if (Date.now() - lastSyncAt < freshness) return;
        try {
          await syncNow(); // pull changes made elsewhere (UI, other clients)
          return;
        } catch (err) {
          log.warn('[API SESSION] Refresh sync failed on kept-alive session; reopening:', err);
          await closeNow();
        }
      }
    }
    await withTimeout(invoke(options.init), t.initMs, 'init');
    open = true;
    openKey = key;
    openedAt = lastSyncAt = Date.now();
  }

  function clearIdleTimer(): void {
    if (idleTimer) { clearTimeout(idleTimer); idleTimer = null; }
  }

  function scheduleIdleClose(): void {
    clearIdleTimer();
    idleTimer = setTimeout(() => { idleTimer = null; void idleClose(); }, keepAliveMs);
    idleTimer.unref?.();
  }

  async function idleClose(): Promise<void> {
    if (!open) return;
    let release: () => void;
    try {
      release = await mutex.acquire(t.lockWaitMs);
    } catch {
      scheduleIdleClose(); // busy — try again later
      return;
    }
    try {
      if (open && Date.now() - lastUsedAt >= keepAliveMs) {
        log.debug(`[API SESSION] Closing session after ${keepAliveMs}ms idle`);
        await closeNow();
      }
    } finally {
      release();
    }
  }

  /**
   * Acquire the lock and run body inside a session context. Cold mode: always
   * shut down afterwards. Keep-alive: leave the session open unless the call
   * asked for (or failed in a way that needs) a clean reopen.
   */
  async function runLocked<T>(body: (ctx: SessionContext) => Promise<T>): Promise<T> {
    const release = await mutex.acquire(t.lockWaitMs);
    clearIdleTimer();
    const ctx: SessionContext = { active: true, closeAfter: false };
    lockHeldSince = Date.now();
    try {
      return await als.run(ctx, () => body(ctx));
    } catch (err) {
      if (err instanceof ApiTimeoutError || !open) ctx.closeAfter = true;
      else lastSyncAt = 0; // force a health-check sync before the next call
      throw err;
    } finally {
      ctx.active = false;
      if (!keepAlive || ctx.closeAfter) {
        await closeNow();
      } else {
        lastUsedAt = Date.now();
        scheduleIdleClose();
      }
      lockHeldSince = null;
      release();
    }
  }

  function withSession<T>(operation: () => Promise<T>, callOptions: SessionCallOptions = {}): Promise<T> {
    if (inSession()) return invoke(operation); // re-entrant: reuse the open session
    return runLocked(async (ctx) => {
      await ensureOpen();
      const result = await withTimeout(invoke(operation), t.operationMs, 'operation');
      if (keepAlive && callOptions.syncAfter) {
        try {
          await syncNow();
        } catch (err) {
          // shutdown() syncs again on close, so the change still gets pushed.
          log.error('[API SESSION] Post-operation sync failed; closing session:', err);
          ctx.closeAfter = true;
        }
      }
      return result;
    });
  }

  async function runWriteBatch(batch: QueuedWrite[]): Promise<void> {
    log.debug(`[WRITE QUEUE] Processing batch of ${batch.length} operation(s)`);
    try {
      await runLocked(async (ctx) => {
        try {
          await ensureOpen();
        } catch (err) {
          log.error('[WRITE QUEUE] Session init failed; rejecting batch:', err);
          for (const w of batch) w.reject(err);
          ctx.closeAfter = true;
          return;
        }
        // Sequential on purpose: the SDK shares one SQLite handle and
        // concurrent writes inside a session have caused contention issues.
        for (const w of batch) {
          try {
            w.resolve(await withTimeout(invoke(w.operation), t.operationMs, 'operation'));
          } catch (err) {
            log.error('[WRITE QUEUE] Operation failed:', err);
            if (err instanceof ApiTimeoutError) ctx.closeAfter = true;
            w.reject(err);
          }
        }
        if (options.sync) {
          try {
            await syncNow();
          } catch (err) {
            // Don't fail the callers: shutdown() also syncs, so close the
            // session now to get the changes pushed.
            log.error('[WRITE QUEUE] Sync failed:', err);
            ctx.closeAfter = true;
          }
        }
      });
    } catch (err) {
      // Lock wait timed out (or something unexpected): fail every unsettled
      // caller. Promise resolve/reject are no-ops for already-settled ones.
      log.error('[WRITE QUEUE] Batch failed:', err);
      for (const w of batch) w.reject(err);
    }
  }

  async function drain(): Promise<void> {
    if (draining) return;
    draining = true;
    try {
      // Writes queued while a batch runs are picked up by the next iteration,
      // so nothing can be left behind waiting for a timer that never fires.
      while (writeQueue.length > 0) {
        const batch = writeQueue;
        writeQueue = [];
        await runWriteBatch(batch);
      }
    } finally {
      draining = false;
      if (writeQueue.length > 0) scheduleDrain();
    }
  }

  function scheduleDrain(): void {
    // Fixed window (not a debounce): a steady stream of writes can't postpone
    // processing indefinitely.
    if (draining || drainTimer) return;
    drainTimer = setTimeout(() => {
      drainTimer = null;
      void drain();
    }, batchDelay);
  }

  function queueWrite<T>(operation: () => Promise<T>): Promise<T> {
    if (inSession()) return invoke(operation); // re-entrant: write in the open session
    return new Promise<T>((resolve, reject) => {
      writeQueue.push({ operation, resolve: resolve as (v: unknown) => void, reject });
      scheduleDrain();
    });
  }

  async function runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    if (inSession()) return invoke(fn);
    const release = await mutex.acquire(t.lockWaitMs);
    try {
      return await withTimeout(invoke(fn), t.operationMs, 'operation');
    } finally {
      release();
    }
  }

  async function close(): Promise<void> {
    clearIdleTimer();
    const ctx = als.getStore();
    if (ctx?.active) { ctx.closeAfter = true; return; } // closes when the current call ends
    const release = await mutex.acquire(t.lockWaitMs);
    try {
      await closeNow();
    } finally {
      release();
    }
  }

  function getState(): ApiSessionState {
    return {
      lockHeld: mutex.isLocked,
      lockWaiters: mutex.waiting,
      pendingWrites: writeQueue.length,
      writeDrainActive: draining,
      activeSessionMs: lockHeldSince === null ? null : Date.now() - lockHeldSince,
      keepAlive,
      sessionOpen: open,
      sessionAgeMs: open ? Date.now() - openedAt : null,
    };
  }

  return { withSession, queueWrite, runExclusive, close, inSession, getState };
}
