const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

const createSharedPool = () => {
  const connectionString = (process.env.DATABASE_URL || '').trim();
  if (!connectionString) {
    throw new Error('DATABASE_URL belum dikonfigurasi untuk shared database bridge');
  }

  const parsePositiveInt = (raw, fallback) => {
    const value = Number.parseInt(`${raw}`, 10);
    if (!Number.isFinite(value) || value <= 0) return fallback;
    return value;
  };

  const min = parsePositiveInt(process.env.DB_POOL_MIN, 4);
  const max = parsePositiveInt(process.env.DB_POOL_MAX, 40);
  const idleTimeoutMs = parsePositiveInt(process.env.DB_IDLE_TIMEOUT_MS, 30 * 1000);
  const connectionTimeoutMs = parsePositiveInt(process.env.DB_CONNECTION_TIMEOUT_MS, 5000);
  const acquireTimeoutMs = parsePositiveInt(process.env.DB_ACQUIRE_TIMEOUT_MS, 10000);
  const statementTimeoutMs = parsePositiveInt(process.env.DB_STATEMENT_TIMEOUT_MS || process.env.STATEMENT_TIMEOUT_MS, 0);
  const safeMax = Math.max(min + 1, max);

  const pool = new Pool({
    connectionString,
    min,
    max: safeMax,
    idleTimeoutMillis: idleTimeoutMs,
    connectionTimeoutMillis: connectionTimeoutMs,
    statement_timeout: statementTimeoutMs > 0 ? statementTimeoutMs : undefined,
  });

  pool.on('connect', (client) => {
    try {
      if (statementTimeoutMs > 0) {
        client.query(`SET statement_timeout = ${statementTimeoutMs}`).catch(() => {});
      }
    } catch (_) {}
  });

  pool.on('acquire', (client) => {
    try {
      client.__goldenityAcquiredAt = Date.now();
    } catch (_) {}
  });

  pool.on('error', (error, client) => {
    console.error('[tenantResolver] Shared pool client error:', {
      message: error?.message || error,
      code: error?.code || null,
      clientProcessId: client?.processID || null,
    });
  });

  pool.on('release', (error, client) => {
    if (!error) return;
    const acquiredAt = client?.__goldenityAcquiredAt || 0;
    const heldMs = acquiredAt > 0 ? Date.now() - acquiredAt : null;
    console.warn('[tenantResolver] Pool client released with error:', {
      message: error?.message || error,
      code: error?.code || null,
      heldMs,
    });
  });

  let poolPendingLogged = 0;
  setInterval(() => {
    try {
      const waiting = pool.waitingCount;
      const total = pool.totalCount;
      const idle = pool.idleCount;
      if ((waiting > 0 && poolPendingLogged < 1) || (waiting > 0 && waiting % 5 === 0)) {
        console.warn('[tenantResolver] Shared pool pressure:', {
          waiting,
          total,
          idle,
          max: safeMax,
        });
        poolPendingLogged += 1;
      } else if (waiting === 0) {
        poolPendingLogged = 0;
      }
    } catch (_) {}
  }, 5000).unref?.();

  global.__goldenitySharedPool = pool;
  return pool;
};

const getAuthToken = (req) => {
  const authorization = req.headers.authorization || '';
  if (!authorization.startsWith('Bearer ')) {
    throw new Error('Authorization header must use Bearer token');
  }
  return authorization.slice(7).trim();
};

const getSharedPool = () => global.__goldenitySharedPool || createSharedPool();

const resolveTenantFromToken = (payload = {}) => {
  const tenantId = (payload.tenantId ?? payload.tenant_id ?? '').toString().trim();
  const tenantSlug = (payload.tenantSlug ?? payload.tenant_slug ?? '').toString().trim();
  return {
    tenantId,
    tenantSlug,
  };
};

const resolveUserContextFromToken = (payload = {}, tenantContext = {}) => {
  const tenantId = (tenantContext.tenantId ?? '').toString().trim();
  const tenantSlug = (tenantContext.tenantSlug ?? '').toString().trim();
  const userId = (
    payload.userId ??
    payload.user_id ??
    payload.id ??
    payload.sub ??
    ''
  )
    .toString()
    .trim();
  const role = (
    payload.role ??
    payload.userRole ??
    payload.user_role ??
    payload.appRole ??
    payload.app_role ??
    'ADMIN'
  )
    .toString()
    .trim();

  return Object.freeze({
    ...payload,
    id: userId || (payload.id ?? '').toString().trim(),
    userId,
    user_id: userId,
    role,
    userRole: role,
    user_role: role,
    appRole: role,
    app_role: role,
    tenantId,
    tenant_id: tenantId,
    tenantSlug,
    tenant_slug: tenantSlug,
  });
};

const tenantResolver = async (req, res, next) => {
  try {
    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret) {
      return res.status(500).json({
        success: false,
        message: 'JWT_SECRET belum dikonfigurasi',
        error: null,
      });
    }

    const token = getAuthToken(req);
    const payload = jwt.verify(token, jwtSecret);

    const { tenantId, tenantSlug } = resolveTenantFromToken(payload);
    if (!tenantId && !tenantSlug) {
      return res.status(401).json({
        success: false,
        message: 'tenantId tidak ditemukan di token',
        error: null,
      });
    }

    const lockedTenantId = tenantId;
    const lockedTenantSlug = tenantSlug;
    if (!lockedTenantId) {
      return res.status(401).json({
        success: false,
        message: 'Tenant ID tidak valid',
        error: null,
      });
    }

    const pool = getSharedPool();

    req.auth = payload;
    req.user = resolveUserContextFromToken(payload, {
      tenantId: lockedTenantId,
      tenantSlug: lockedTenantSlug,
    });
    req.tenant = { tenantId: lockedTenantId, slug: lockedTenantSlug };
    req.tenantDb = pool;
    req.db = pool;

    return next();
  } catch (error) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized',
      error: error.message,
    });
  }
};

module.exports = {
  tenantResolver,
  getSharedPool,
};
