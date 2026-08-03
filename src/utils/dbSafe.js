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

const installQuerySerializer = (client) => {
  if (!client || client.__goldenityQuerySerializerInstalled) return client;
  client.__goldenityQuerySerializerInstalled = true;
  const originalQuery = client.query.bind(client);
  let head = Promise.resolve();
  let inflight = 0;
  client.query = function serializedQuery(...args) {
    const runNow = () => {
      inflight += 1;
      let result;
      try {
        result = originalQuery(...args);
      } catch (err) {
        inflight = Math.max(0, inflight - 1);
        throw err;
      }
      const promise = Promise.resolve(result);
      return promise.finally(() => {
        inflight = Math.max(0, inflight - 1);
      });
    };
    if (inflight <= 0) {
      head = runNow();
      return head;
    }
    const next = head.then(runNow, runNow);
    head = next.catch(() => {});
    return next;
  };
  return client;
};

const normalizeCartItemInPlace = (item) => {
  if (!item || typeof item !== 'object' || Array.isArray(item)) {
    return item;
  }
  const nested =
    item.product && typeof item.product === 'object' && !Array.isArray(item.product)
      ? item.product
      : null;
  const obj = item;
  if (nested) {
    const pickFirst = (keys) => {
      for (const k of keys) {
        if (nested[k] !== undefined && nested[k] !== null && `${nested[k]}`.trim() !== '') {
          return nested[k];
        }
      }
      return undefined;
    };
    const nestedId = pickFirst(['id', 'product_id', 'productId', 'entity_id', 'manual_item_id']);
    const nestedName = pickFirst(['name', 'product_name', 'productName', 'title', 'label']);
    const nestedQty = pickFirst(['qty', 'quantity', 'jumlah']);
    const nestedPrice = pickFirst([
      'custom_price',
      'customPrice',
      'price',
      'unit_price',
      'unitPrice',
      'harga_jual',
      'sale_price',
      'harga',
    ]);
    const nestedIsService = pickFirst([
      'is_service',
      'isService',
      'is_custom_item',
      'isCustomItem',
      'isNonStock',
      'is_non_stock',
    ]);
    const nestedNote = pickFirst([
      'note',
      'item_note',
      'product_note',
      'notes',
      'remark',
      'remarks',
      'catatan',
    ]);
    if (nestedId !== undefined && (obj.id === undefined || `${obj.id}`.length > 14)) {
      try {
        obj.id = nestedId;
      } catch (_) {}
    }
    if (nestedId !== undefined) {
      try {
        if (obj.product_id === undefined || obj.product_id === null || `${obj.product_id}`.trim() === '') {
          obj.product_id = nestedId;
        }
        if (obj.productId === undefined || obj.productId === null || `${obj.productId}`.trim() === '') {
          obj.productId = nestedId;
        }
      } catch (_) {}
    }
    if (nestedName !== undefined) {
      try {
        if (obj.product_name === undefined || obj.product_name === null || `${obj.product_name}`.trim() === '') {
          obj.product_name = nestedName;
        }
        if (obj.productName === undefined || obj.productName === null || `${obj.productName}`.trim() === '') {
          obj.productName = nestedName;
        }
        if (obj.name === undefined || obj.name === null || `${obj.name}`.trim() === '') {
          obj.name = nestedName;
        }
      } catch (_) {}
    }
    if (nestedQty !== undefined && Number.isFinite(Number(nestedQty))) {
      try {
        const qtyNum = Number(nestedQty);
        if ((obj.qty === undefined || !Number.isFinite(Number(obj.qty)) || Number(obj.qty) <= 0) && qtyNum > 0) {
          obj.qty = qtyNum;
        }
        if ((obj.quantity === undefined || !Number.isFinite(Number(obj.quantity)) || Number(obj.quantity) <= 0) && qtyNum > 0) {
          obj.quantity = qtyNum;
        }
      } catch (_) {}
    }
    if (nestedPrice !== undefined && Number.isFinite(Number(nestedPrice))) {
      try {
        const pr = Number(nestedPrice);
        if ((obj.price === undefined || !Number.isFinite(Number(obj.price)) || Number(obj.price) <= 0) && pr > 0) {
          obj.price = pr;
        }
        if ((obj.unit_price === undefined || !Number.isFinite(Number(obj.unit_price)) || Number(obj.unit_price) <= 0) && pr > 0) {
          obj.unit_price = pr;
        }
        if ((obj.unitPrice === undefined || !Number.isFinite(Number(obj.unitPrice)) || Number(obj.unitPrice) <= 0) && pr > 0) {
          obj.unitPrice = pr;
        }
        if ((obj.harga_jual === undefined || !Number.isFinite(Number(obj.harga_jual)) || Number(obj.harga_jual) <= 0) && pr > 0) {
          obj.harga_jual = pr;
        }
        if ((obj.custom_price === undefined || !Number.isFinite(Number(obj.custom_price)) || Number(obj.custom_price) <= 0) && pr > 0) {
          obj.custom_price = pr;
        }
        if ((obj.customPrice === undefined || !Number.isFinite(Number(obj.customPrice)) || Number(obj.customPrice) <= 0) && pr > 0) {
          obj.customPrice = pr;
        }
      } catch (_) {}
    }
    if (nestedIsService !== undefined && nestedIsService !== null) {
      try {
        if (obj.is_service === undefined || obj.is_service === null) {
          obj.is_service = nestedIsService;
        }
        if (obj.isService === undefined || obj.isService === null) {
          obj.isService = nestedIsService;
        }
      } catch (_) {}
    }
    if (nestedNote !== undefined && `${nestedNote}`.trim() !== '') {
      try {
        if (obj.note === undefined || obj.note === null || `${obj.note}`.trim() === '') {
          obj.note = nestedNote;
        }
        if (obj.item_note === undefined || obj.item_note === null || `${obj.item_note}`.trim() === '') {
          obj.item_note = nestedNote;
        }
        if (obj.notes === undefined || obj.notes === null || `${obj.notes}`.trim() === '') {
          obj.notes = nestedNote;
        }
      } catch (_) {}
    }
  }
  if (
    obj.id !== undefined &&
    obj.id !== null &&
    `${obj.id}`.length >= 13 &&
    /^\d+$/.test(`${obj.id}`) &&
    (obj.product_id === undefined || `${obj.product_id}`.trim() === '' || `${obj.product_id}` === `${obj.id}`) &&
    (obj.productId === undefined || `${obj.productId}`.trim() === '' || `${obj.productId}` === `${obj.id}`)
  ) {
    const numericId = Number(obj.id);
    if (numericId > 1e12) {
      if (nested && nested.id !== undefined && nested.id !== null && `${nested.id}` !== `${obj.id}`) {
        try {
          obj.id = nested.id;
          obj.product_id = nested.id;
          obj.productId = nested.id;
        } catch (_) {}
      }
    }
  }
  return obj;
};

const normalizePayloadCartItems = (req) => {
  if (!req || !req.body || typeof req.body !== 'object') return req;
  const payload = req.body;
  const candidates = ['items', 'products', 'cart_items', 'cartItems', 'sales_items', 'salesItems', 'order_items', 'orderItems', 'detail'];
  for (const key of candidates) {
    const arr = payload[key];
    if (Array.isArray(arr)) {
      for (let i = 0; i < arr.length; i += 1) {
        normalizeCartItemInPlace(arr[i]);
      }
    }
  }
  if (payload.record && Array.isArray(payload.record?.items)) {
    for (let i = 0; i < payload.record.items.length; i += 1) {
      normalizeCartItemInPlace(payload.record.items[i]);
    }
  }
  if (Array.isArray(payload.records)) {
    for (let r = 0; r < payload.records.length; r += 1) {
      const rec = payload.records[r];
      for (const key of candidates) {
        const arr = rec && rec[key];
        if (Array.isArray(arr)) {
          for (let i = 0; i < arr.length; i += 1) normalizeCartItemInPlace(arr[i]);
        }
      }
    }
  }
  if (Array.isArray(payload)) {
    for (let r = 0; r < payload.length; r += 1) {
      const rec = payload[r];
      for (const key of candidates) {
        const arr = rec && rec[key];
        if (Array.isArray(arr)) {
          for (let i = 0; i < arr.length; i += 1) normalizeCartItemInPlace(arr[i]);
        }
      }
    }
  }
  return req;
};

const isPgPool = (poolOrClient) =>
  !!poolOrClient
  && typeof poolOrClient === 'object'
  && typeof poolOrClient.query === 'function'
  && typeof poolOrClient.connect === 'function'
  && typeof poolOrClient.release !== 'function';


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

const getClientFromPool = async (pool) => {
  if (pool && typeof pool === 'object' && typeof pool.release === 'function' && !isPgPool(pool)) {
    return installQuerySerializer(pool);
  }
  return withRetries(
    async () => {
      const client = await pool.connect();
      if (!client) {
        throw new Error('Pool returned empty client');
      }
      return installQuerySerializer(client);
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
};

const runTransaction = async (poolOrClient, handler, options = {}) => {
  const outerClient = isPgPool(poolOrClient)
    ? await getClientFromPool(poolOrClient)
    : poolOrClient;
  const release = isPgPool(poolOrClient)
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
  installQuerySerializer,
  normalizeCartItemInPlace,
  normalizePayloadCartItems,
};
