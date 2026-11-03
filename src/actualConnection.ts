import fs from 'fs';
import path from 'path';
import os from 'os';
import api from '@actual-app/api';
import logger from './logger.js';
import config from './config.js';
import { connectionPool } from './lib/ActualConnectionPool.js';
import { withActualApi, runExclusiveApi } from './lib/actual-adapter.js';
import { createModuleLogger } from './lib/loggerFactory.js';

const log = createModuleLogger('CONNECTION');

const DEFAULT_DATA_DIR = path.resolve(os.homedir() || '.', '.actual');

let initialized = false;
let initializing = false;
let initializationError: Error | null = null;
// Feature flag to enable connection pooling - can be disabled via environment variable
let useConnectionPool = process.env.USE_CONNECTION_POOL !== 'false';

export async function connectToActual() {
  if (initialized) return;
  if (initializing) {
    while (initializing) await new Promise(r => setTimeout(r, 100));
    if (initializationError) throw initializationError;
    return;
  }
  initializing = true;

  try {
  const SERVER_URL = config.ACTUAL_SERVER_URL;
  const BUDGET_SYNC_ID = config.ACTUAL_BUDGET_SYNC_ID;
  const TEST_ACTUAL_CONNECTION = process.argv.includes('--test-actual-connection');

  // Use configured MCP_BRIDGE_DATA_DIR (fallback to DEFAULT_DATA_DIR) for all runs
  const DATA_DIR = config.MCP_BRIDGE_DATA_DIR || DEFAULT_DATA_DIR;

  new URL(SERVER_URL);

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  log.info(`Verifying Actual API connection (dataDir=${DATA_DIR}, budget=${BUDGET_SYNC_ID})`);

    // Open and close one session through the shared API session lock
    // (init → downloadBudget → shutdown). Calling api.init() directly here would
    // re-initialise the singleton underneath any in-flight tool call, and leaving
    // it open is pointless: every tool call opens its own session anyway.
    await withActualApi(async () => {});

    if (TEST_ACTUAL_CONNECTION) {
      logger.info('Test flag detected (--test-actual-connection) — connection verified, exiting.');
      process.exit(0);
    }

    initialized = true;
    logger.info('✅ Connected to Actual Finance and downloaded budget');
  } catch (err) {
    initializationError = err instanceof Error ? err : new Error(String(err));
    logger.error('❌ Failed to connect to Actual Finance:', initializationError);
    throw initializationError;
  } finally {
    initializing = false;
  }
}

export async function shutdownActual() {
  if (useConnectionPool) {
    await connectionPool.shutdownAll();
    initialized = false;
    return;
  }
  
  try {
    const maybeApi = api as unknown as { shutdown?: () => Promise<unknown> };
    if (typeof maybeApi.shutdown === 'function') {
      // Under the session lock so an in-flight tool call can finish first.
      await runExclusiveApi(async () => { await maybeApi.shutdown!(); });
    }
    initialized = false;
    logger.info('Actual API shutdown complete.');
  } catch (err) {
    logger.error('Error during Actual API shutdown:', err);
  }
}

/**
 * Initialize connection for a specific MCP session
 * Uses connection pooling to give each session its own Actual Budget connection
 */
export async function connectToActualForSession(sessionId: string) {
  if (!useConnectionPool) {
    // Fallback to shared connection
    return connectToActual();
  }
  
  try {
    // Ensure connection pool initialization is complete before accepting connections
    await connectionPool.waitForInitialization();
    await connectionPool.getConnection(sessionId);
    logger.info(`Actual API connection ready for session: ${sessionId}`);
  } catch (err) {
    logger.error(`Failed to connect to Actual for session ${sessionId}:`, err);
    throw err;
  }
}

/**
 * Shutdown connection for a specific MCP session
 */
export async function shutdownActualForSession(sessionId: string) {
  if (!useConnectionPool) {
    return;
  }
  
  try {
    await connectionPool.shutdownConnection(sessionId);
    logger.info(`Actual API connection shutdown for session: ${sessionId}`);
  } catch (err) {
    logger.error(`Error shutting down Actual for session ${sessionId}:`, err);
  }
}

export function getConnectionState() {
  // When using connection pooling, consider server initialized if pool is ready
  // (even with 0 active connections, pool being initialized means server is ready to accept sessions)
  const isPoolInitialized = useConnectionPool && connectionPool.isInitialized();
  const effectivelyInitialized = useConnectionPool ? isPoolInitialized : initialized;
  
  return {
    initialized: effectivelyInitialized,
    initializationError,
    connectionPool: useConnectionPool ? connectionPool.getStats() : null,
    idleTimeoutMinutes: useConnectionPool ? connectionPool.getIdleTimeoutMinutes() : null,
  };
}

export function canAcceptNewSession(): boolean {
  if (!useConnectionPool) {
    return true; // Shared connection mode - always accept
  }
  return connectionPool.canAcceptNewSession();
}
