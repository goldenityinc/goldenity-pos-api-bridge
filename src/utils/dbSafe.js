const fs = require('fs');
const path = require('path');
const { once } = require('events');

const TRANSIENT_SQLSTATE = new Set([
  '40001',
  '40P01',
  '53000',
  '53100',
  '53200',
  '53300',
  '53400',
  '08000',
  '08003',
  '08004',
  '08006',
  '08007',
  '57014',
  '57P01',
  '57P02',
  '57P03',
  '57P04',
  'HV00R',
  'HV00L',
  'HV00N',
  'HV00J',
  '28000',
]);

const isTransientDbError = (error) => {
  if (!error) return false;
  const code = (error.code || error.sqlState || error.sqlstate || '').toString().trim().toUpperCase();
  if (code && TRANSIENT_SQLSTATE.has(code)) return true;
  const message = `${error.message || ''}`.toLowerCase();
  if (/\bdeadlock\b|\bserialization\b|\bconnection\b.*\b(?:reset|lost|refused|closed|timed out|timeout)\b/.test(message)) {
    return true;
  }
  return Boolean(
    error.code && /^ECONN|ETIMEDOUT|EHOSTUNREACH|ENOTFOUND|EPIPE|ERR_SOCKET|ERR_TLS/.test(error.code.toString()),
  );
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const withRetries = async (task, options = {}) => {
  const maxAttempts = Number.isFinite(Number(options.maxAttempts))
    ? Math.max(1, Math.min(Number(options.maxAttempts), 10))
    : 3;
  const baseDelay = Number.isFinite(Number(options.baseDelayMs))
    ? Math.max(0, Number(options.baseDelayMs))
    : 120;
  const maxDelay = Number.isFinite(Number(options.maxDelayMs))
    ? Math.max(baseDelay, Number(options.maxDelayMs))
    : 2000;
  const label = options.label || 'db_operation';
  const shouldRetry = typeof options.shouldRetry === 'function' ? options.shouldRetry : isTransientDbError;

  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await task(attempt);
    } catch (error) {
      lastError = error;
      const retryable = attempt < maxAttempts && shouldRetry(error, attempt);
      if (!retryable) {
        throw error;
      }
      const backoffMs = Math.min(maxDelay, baseDelay * (2 ** (attempt - 1)));
      const jitterMs = Math.floor(backoffMs * 0.2 * Math.random());
      const delay = backoffMs + jitterMs;
      console.warn(
        `[dbSafe] Retrying transient error for ${label} (attempt=${attempt}/${maxAttempts}, delay=${delay}ms)`,
        {
          code: error?.code,
          message: error?.message,
        },
      );
      await sleep(delay);
    }
  }
  throw lastError;
};

const getClientFromPool = async (pool) => withRetries(
  async () => {
    const client = await pool.connect();
    if (!client) {
      throw new Error('Pool returned empty client');
    }
    return client;
  },
  {
    label: 'pool_connect',
    maxAttempts: 5,
    baseDelayMs: 200,
    maxDelayMs: 3000,
    shouldRetry: (error) => {
      if (isTransientDbError(error)) return true;
      const code = error?.code?.toString();
      return !!(code && /^pool|timeout|timedout/i.test(code));
    },
  },
);

const runTransaction = async (poolOrClient, handler, options = {}) => {
  const outerClient = poolOrClient && typeof poolOrClient.connect === 'function'
    ? await getClientFromPool(poolOrClient)
    : poolOrClient;
  const release = poolOrClient && typeof poolOrClient.connect === 'function'
    ? () => outerClient.release()
    : () => {};
  const supportsTransactions = Boolean(options.supportsTransactions !== false);

  return withRetries(
    async () => {
      let committed = false;
      try {
        if (supportsTransactions) {
          await outerClient.query('BEGIN');
        }
        const result = await handler(outerClient);
        if (supportsTransactions) {
          await outerClient.query('COMMIT');
        }
        committed = true;
        return result;
      } catch (error) {
        if (supportsTransactions && !committed) {
          try {
            await outerClient.query('ROLLBACK');
          } catch (rollbackError) {
            console.warn('[dbSafe] Transaction rollback failed:', rollbackError?.message || rollbackError);
          }
        }
        throw error;
      }
    },
    {
      label: options.label || 'transaction',
      maxAttempts: options.maxAttempts || 4,
      baseDelayMs: options.baseDelayMs || 150,
      maxDelayMs: options.maxDelayMs || 2500,
      shouldRetry: (error, attempt) => {
        if (typeof options.shouldRetry === 'function') {
          return !!options.shouldRetry(error, attempt);
        }
        return isTransientDbError(error);
      },
    },
  ).finally(() => release());
};

const resolveLogDir = () => {
  const configured = (process.env.FAILED_PAYLOAD_LOG_DIR || '').toString().trim();
  if (configured) {
    return configured;
  }
  return path.resolve(process.cwd(), 'logs', 'failed_payloads');
};

let logDirPromise = null;
const ensureLogDir = () => {
  if (!logDirPromise) {
    logDirPromise = fs.promises.mkdir(resolveLogDir(), { recursive: true });
  }
  return logDirPromise.catch(() => {});
};

const getSafeLogToken = (value) => `${value || ''}`
  .toString()
  .replace(/[^a-zA-Z0-9_-]/g, '')
  .slice(0, 48) || 'unknown';

const storeFailedPayload = async (kind, payload, error, extra = {}) => {
  try {
    await ensureLogDir();
    const logPath = path.join(resolveLogDir(), `${getSafeLogToken(kind)}.jsonl`);
    const safePayload = (() => {
      try {
        return JSON.parse(JSON.stringify(payload || {}));
      } catch (_) {
        return { raw: `${payload || ''}` };
      }
    })();
    const safeError = {
      message: error?.message || String(error || ''),
      code: error?.code || null,
      constraint: error?.constraint || null,
      stack: error?.stack || null,
    };
    const line = {
      timestamp: new Date().toISOString(),
      kind,
      payload: safePayload,
      error: safeError,
      extra: typeof extra === 'object' && extra ? extra : {},
    };
    await fs.promises.appendFile(logPath, `${JSON.stringify(line)}\n`, 'utf8');
    return logPath;
  } catch (storageError) {
    console.error('[dbSafe] Failed to store failed payload on disk:', {
      kind,
      storageError: storageError?.message || storageError,
      originalMessage: error?.message || error,
    });
    return null;
  }
};

module.exports = {
  TRANSIENT_SQLSTATE,
  isTransientDbError,
  withRetries,
  sleep,
  getClientFromPool,
  runTransaction,
  storeFailedPayload,
};
