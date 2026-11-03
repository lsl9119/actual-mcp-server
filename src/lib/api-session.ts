/**
 * Actual API session coordinator.
 *
 * `@actual-app/api` is a process-wide singleton backed by a single SQLite
 * connection: only one init → work → shutdown cycle may be in flight at a time.
 * This module owns that invariant and guarantees the server can never wedge
 * permanently, whatever the Actual server or SDK does:
 *
 *  - **FIFO mutex with a wait timeout** — callers that cannot get the session
 *    within `lockWaitMs` fail fast instead of queueing forever.
 *  - **Bounded hold time** — init/download, the operation, sync and shutdown
 *    each have their own timeout, so a hung SDK call releases the lock.
 *  - **Re-entrancy** — a call made from inside an active session (e.g. a write
 *    operation that calls a read helper) runs inline in that session instead of
 *    deadlocking on the lock it already holds.
 *  - **Write batching without stranding** — writes that arrive within
 *    `writeBatchDelayMs` share one session; a single drain loop guarantees every
 *    queued write is processed, even when it arrives mid-batch.
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
  /** Max time for the post-write api.sync(). */
  syncMs: number;
  /** Max time for api.shutdown(). */
  shutdownMs: number;
}

export const DEFAULT_API_SESSION_TIMEOUTS: ApiSessionTimeouts = {
  lockWaitMs: 120_000,
  initMs: 60_000,
  operationMs: 120_000,
  syncMs: 60_000,
  shutdownMs: 30_000,
};

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
  /** Pushes local changes to the server after a write batch (api.sync). */
  sync?: () => Promise<void>;
  /** Timeouts in ms; 0 or a negative value disables that timeout. */
  timeouts?: Partial<ApiSessionTimeouts>;
  /** How long the first queued write waits for others to join its batch. */
  writeBatchDelayMs?: number;
  logger?: SessionLogger;
}

export type ApiTimeoutPhase = 'lock-wait' | 'init' | 'operation' | 'sync' | 'shutdown';

export class ApiTimeoutError extends Error {
  readonly code = 'ETIMEDOUT';
  constructor(readonly phase: ApiTimeoutPhase, readonly timeoutMs: number, message: string) {
    super(message);
    this.name = 'ApiTimeoutError';
  }
}

function timeoutMessage(phase: ApiTimeoutPhase, ms: number, extra = ''): string {
  switch (phase) {
    case 'lock-wait':
      return `Server busy: waited ${ms}ms for the Actual Budget session${extra}. ` +
        'Other requests are still running; retry shortly (or raise ACTUAL_LOCK_WAIT_TIMEOUT_MS).';
    case 'init':
      return `Timed out after ${ms}ms connecting to Actual Budget / downloading the budget. ` +
        'Check that ACTUAL_SERVER_URL is reachable, then retry (or raise ACTUAL_INIT_TIMEOUT_MS).';
    case 'operation':
      return `Actual Budget operation timed out after ${ms}ms and was abandoned so other requests can proceed. ` +
        'If this was a write it may still have been applied — verify before retrying (or raise ACTUAL_OPERATION_TIMEOUT_MS).';
    case 'sync':
      return `Timed out after ${ms}ms syncing changes to the Actual Budget server (ACTUAL_SYNC_TIMEOUT_MS).`;
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

interface SessionContext { active: boolean }

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
  /** How long the current session has held the lock (ms), or null if idle. */
  activeSessionMs: number | null;
}

export interface ApiSessionManager {
  /** Run a (read) operation in its own init → op → shutdown session. */
  withSession<T>(operation: () => Promise<T>): Promise<T>;
  /** Queue a write; writes arriving together share one session and one sync. */
  queueWrite<T>(operation: () => Promise<T>): Promise<T>;
  /** Hold the lock without opening a session (e.g. for a bare api.shutdown()). */
  runExclusive<T>(fn: () => Promise<T>): Promise<T>;
  /** True when called from inside an active session. */
  inSession(): boolean;
  getState(): ApiSessionState;
}

export function createApiSessionManager(options: ApiSessionOptions): ApiSessionManager {
  const t: ApiSessionTimeouts = { ...DEFAULT_API_SESSION_TIMEOUTS, ...(options.timeouts ?? {}) };
  const batchDelay = options.writeBatchDelayMs ?? 100;
  const log = options.logger ?? noopLogger;
  const mutex = new TimedMutex();
  const als = new AsyncLocalStorage<SessionContext>();
  let sessionStartedAt: number | null = null;

  let writeQueue: QueuedWrite[] = [];
  let draining = false;
  let drainTimer: NodeJS.Timeout | null = null;

  // A context only counts while its session is open; callbacks that outlive
  // their session (timers, abandoned promises) must queue like everyone else.
  const inSession = () => als.getStore()?.active === true;

  async function safeShutdown(): Promise<void> {
    try {
      await withTimeout(invoke(options.shutdown), t.shutdownMs, 'shutdown');
    } catch (err) {
      log.error('[API SESSION] shutdown failed; releasing the session lock anyway:', err);
    }
  }

  /** Acquire the lock, run body inside a session context, always shut down and release. */
  async function runLocked<T>(body: () => Promise<T>): Promise<T> {
    const release = await mutex.acquire(t.lockWaitMs);
    const ctx: SessionContext = { active: true };
    sessionStartedAt = Date.now();
    try {
      return await als.run(ctx, body);
    } finally {
      ctx.active = false;
      await safeShutdown();
      sessionStartedAt = null;
      release();
    }
  }

  function withSession<T>(operation: () => Promise<T>): Promise<T> {
    if (inSession()) return invoke(operation); // re-entrant: reuse the open session
    return runLocked(async () => {
      await withTimeout(invoke(options.init), t.initMs, 'init');
      return await withTimeout(invoke(operation), t.operationMs, 'operation');
    });
  }

  async function runWriteBatch(batch: QueuedWrite[]): Promise<void> {
    log.debug(`[WRITE QUEUE] Processing batch of ${batch.length} operation(s)`);
    try {
      await runLocked(async () => {
        try {
          await withTimeout(invoke(options.init), t.initMs, 'init');
        } catch (err) {
          log.error('[WRITE QUEUE] Session init failed; rejecting batch:', err);
          for (const w of batch) w.reject(err);
          return;
        }
        // Sequential on purpose: the SDK shares one SQLite handle and
        // concurrent writes inside a session have caused contention issues.
        for (const w of batch) {
          try {
            w.resolve(await withTimeout(invoke(w.operation), t.operationMs, 'operation'));
          } catch (err) {
            log.error('[WRITE QUEUE] Operation failed:', err);
            w.reject(err);
          }
        }
        if (options.sync) {
          try {
            await withTimeout(invoke(options.sync), t.syncMs, 'sync');
          } catch (err) {
            // Don't fail the callers: shutdown() also persists local changes.
            log.error('[WRITE QUEUE] Sync failed:', err);
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

  function getState(): ApiSessionState {
    return {
      lockHeld: mutex.isLocked,
      lockWaiters: mutex.waiting,
      pendingWrites: writeQueue.length,
      writeDrainActive: draining,
      activeSessionMs: sessionStartedAt === null ? null : Date.now() - sessionStartedAt,
    };
  }

  return { withSession, queueWrite, runExclusive, inSession, getState };
}
