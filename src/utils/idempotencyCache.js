const DEFAULT_TTL_MS = 5 * 60 * 1000;
const MAX_CACHE_SIZE = 500;

const responseCache = new Map();

const normalizeHeaderValue = (value) => {
  if (Array.isArray(value)) {
    return normalizeHeaderValue(value[0]);
  }
  return value == null ? '' : value.toString().trim();
};

const clonePayload = (payload) => {
  if (payload == null) {
    return payload;
  }

  try {
    return JSON.parse(JSON.stringify(payload));
  } catch (_) {
    return payload;
  }
};

const pruneExpiredEntries = (now = Date.now()) => {
  for (const [key, entry] of responseCache.entries()) {
    if ((entry?.expiresAt || 0) <= now) {
      responseCache.delete(key);
    }
  }

  while (responseCache.size > MAX_CACHE_SIZE) {
    const oldestKey = responseCache.keys().next().value;
    if (!oldestKey) {
      break;
    }
    responseCache.delete(oldestKey);
  }
};

const resolveIdempotencyKey = (req) => {
  return normalizeHeaderValue(
    req.headers['x-idempotency-key'] || req.headers['idempotency-key'],
  );
};

const buildCacheKey = (req, table, rawKey) => {
  const tenantId = normalizeHeaderValue(
    req.headers['x-tenant-id'] || req.tenantId || req.user?.tenantId,
  );
  return `${tenantId || 'default'}:${table}:${rawKey}`;
};

const getCachedResponse = (req, table) => {
  const rawKey = resolveIdempotencyKey(req);
  if (!rawKey) {
    return null;
  }

  pruneExpiredEntries();

  const cacheKey = buildCacheKey(req, table, rawKey);
  const entry = responseCache.get(cacheKey);
  if (!entry) {
    return null;
  }

  if ((entry.expiresAt || 0) <= Date.now()) {
    responseCache.delete(cacheKey);
    return null;
  }

  return clonePayload(entry.payload);
};

const storeResponse = (req, table, payload, ttlMs = DEFAULT_TTL_MS) => {
  const rawKey = resolveIdempotencyKey(req);
  if (!rawKey) {
    return;
  }

  pruneExpiredEntries();

  const cacheKey = buildCacheKey(req, table, rawKey);
  responseCache.set(cacheKey, {
    payload: clonePayload(payload),
    expiresAt: Date.now() + ttlMs,
  });
};

module.exports = {
  getCachedResponse,
  storeResponse,
};