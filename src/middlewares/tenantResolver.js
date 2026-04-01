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
