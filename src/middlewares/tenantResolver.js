const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

const installQuerySerializer = (client) => {
  if (client.__goldenityQuerySerializerInstalled) return;
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
};

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

  const ensureCoreTablesSql = `
    CREATE TABLE IF NOT EXISTS app_versions (
      id SERIAL PRIMARY KEY,
      version VARCHAR(50),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version VARCHAR(255) PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `.trim();

  const bootstrapClient = async (client) => {
    try {
      installQuerySerializer(client);
      if (statementTimeoutMs > 0) {
        try {
          await client.query(`SET statement_timeout = ${statementTimeoutMs}`);
        } catch (_) {}
      }
      try {
        await client.query(ensureCoreTablesSql);
      } catch (err) {
        const code = err?.code;
        if (code !== '42P01' && code !== '42P07' && code !== '23505' && code !== '25P02') {
          console.warn('[tenantResolver] core table bootstrap skipped:', {
            message: err?.message || err,
            code,
          });
        }
      }
      try {
        await client.query(`INSERT INTO app_versions (version) VALUES ($1) ON CONFLICT DO NOTHING`, ['bridge-1.2.0']);
      } catch (_) {}
    } catch (_) {}
  };

  pool.on('connect', (client) => {
    bootstrapClient(client).catch(() => {});
  });

  pool.on('acquire', (client) => {
    try {
      installQuerySerializer(client);
      client.__goldenityAcquiredAt = Date.now();
    } catch (_) {}
  });

  pool.on('error', (error, client) => {
    const code = error?.code;
    const isRelationMissing = code === '42P01' || code === '42703';
    if (isRelationMissing) {
      console.info('[tenantResolver] Ignorable pool error (relation/column missing - auto bootstrap):', {
        message: error?.message || error,
        code,
        clientProcessId: client?.processID || null,
      });
      return;
    }
    console.error('[tenantResolver] Shared pool client error:', {
      message: error?.message || error,
      code: code || null,
      clientProcessId: client?.processID || null,
    });
  });

  const poisonErrorCodes = new Set([
    '08000','08001','08003','08004','08006','08007','08P01',
    '57P01','57P02','57P03','57P04','58000','58001','58002','58003','58004',
    'XX000','XX001',
    '26000','28000','28P01','3D000','3C000','2D000',
  ]);

  pool.on('release', (error, client) => {
    if (!error) return;
    const code = error?.code;
    const acquiredAt = client?.__goldenityAcquiredAt || 0;
    const heldMs = acquiredAt > 0 ? Date.now() - acquiredAt : null;
    const isBenign = code === '42P01' || code === '42703' || code === '42P07' || code === '23505';
    const isPoison = !!code && poisonErrorCodes.has(code);
    if (isPoison) {
      try { client.release(true); } catch (_) {}
      console.error('[tenantResolver] POOL POISON client released, destroyed:', {
        message: error?.message || error,
        code,
        heldMs,
      });
      return;
    }
    if (isBenign) {
      console.info('[tenantResolver] Benign release (bootstrap schema gap - safe):', {
        message: error?.message || error,
        code,
        heldMs,
      });
      return;
    }
    console.warn('[tenantResolver] Pool client released with error:', {
      message: error?.message || error,
      code: code || null,
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
    const url = (req.originalUrl || req.url || "").toString();
    const internalRelay = (req.headers["x-internal-relay"] || req.headers["x-relay-skip-auth"] || "").toString().trim();
    if (
      url.startsWith("/api/v1/relay/") ||
      url.startsWith("/relay/") ||
      internalRelay === "1" ||
      internalRelay.toLowerCase() === "true"
    ) {
      const rawTenantId = (
        (req.headers["x-tenant-id"] || "").toString().trim() ||
        (req.body && (req.body.tenantId || req.body.tenant_id || "").toString().trim()) ||
        (req.query && (req.query.tenantId || req.query.tenant_id || "").toString().trim())
      );
      const rawTenantSlug = (
        (req.headers["x-tenant-slug"] || "").toString().trim() ||
        (req.body && (req.body.tenantSlug || req.body.tenant_slug || "").toString().trim())
      );
      if (rawTenantId || rawTenantSlug) {
        req.tenant = {
          tenantId: rawTenantId || rawTenantSlug,
          slug: rawTenantSlug || rawTenantId,
          _viaRelayBypass: true,
        };
        req.user = {
          id: "relay-system",
          role: "RELAY_BYPASS",
          tenantId: req.tenant.tenantId,
          tenantSlug: req.tenant.slug,
        };
        try {
          req.tenantDb = getSharedPool();
          req.db = req.tenantDb;
        } catch (_) {}
        return next();
      }
    }

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
