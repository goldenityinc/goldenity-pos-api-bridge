const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

const createSharedPool = () => {
  const connectionString = (process.env.DATABASE_URL || '').trim();
  if (!connectionString) {
    throw new Error('DATABASE_URL belum dikonfigurasi untuk shared database bridge');
  }

  const pool = new Pool({ connectionString });
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

    const pool = getSharedPool();
    const tenantLookup = await pool.query(
      `SELECT id, slug, is_active
       FROM tenants
       WHERE (id = $1 OR slug = $2)
       LIMIT 1`,
      [tenantId || null, tenantSlug || null],
    );

    const tenantRow = tenantLookup.rows?.[0] || null;
    if (!tenantRow) {
      return res.status(401).json({
        success: false,
        message: 'Tenant tidak ditemukan',
        error: null,
      });
    }

    if (tenantRow.is_active === false) {
      return res.status(403).json({
        success: false,
        message: 'Tenant tidak aktif',
        error: null,
      });
    }

    const lockedTenantId = (tenantRow.id ?? '').toString().trim();
    const lockedTenantSlug = (tenantRow.slug ?? '').toString().trim();
    if (!lockedTenantId) {
      return res.status(401).json({
        success: false,
        message: 'Tenant ID tidak valid',
        error: null,
      });
    }

    req.auth = payload;
    req.user = Object.freeze({
      ...payload,
      tenantId: lockedTenantId,
      tenant_id: lockedTenantId,
      tenantSlug: lockedTenantSlug,
      tenant_slug: lockedTenantSlug,
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
