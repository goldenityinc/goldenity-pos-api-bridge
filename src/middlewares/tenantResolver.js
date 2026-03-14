const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

const pools = global.__goldenityTenantPools || (global.__goldenityTenantPools = new Map());

const getAuthToken = (req) => {
  const authorization = req.headers.authorization || '';
  if (!authorization.startsWith('Bearer ')) {
    throw new Error('Authorization header must use Bearer token');
  }
  return authorization.slice(7).trim();
};

const getOrCreateTenantPool = (tenantId, dbUrl) => {
  const existing = pools.get(tenantId);

  if (!existing) {
    const pool = new Pool({ connectionString: dbUrl });
    pools.set(tenantId, { dbUrl, pool });
    return pool;
  }

  if (existing.dbUrl !== dbUrl) {
    const pool = new Pool({ connectionString: dbUrl });
    pools.set(tenantId, { dbUrl, pool });
    void existing.pool.end().catch(() => {});
    return pool;
  }

  return existing.pool;
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

    const tenantId = payload?.tenantId || payload?.tenant_id;
    const dbUrl = payload?.dbUrl;

    if (!tenantId || typeof tenantId !== 'string') {
      return res.status(401).json({
        success: false,
        message: 'tenantId tidak ditemukan di token',
        error: null,
      });
    }

    if (!dbUrl || typeof dbUrl !== 'string') {
      return res.status(401).json({
        success: false,
        message: 'dbUrl tidak ditemukan di token',
        error: null,
      });
    }

    const pool = getOrCreateTenantPool(tenantId, dbUrl);
    await pool.query('SELECT 1');

    req.auth = payload;
    req.tenant = { tenantId, dbUrl };
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
  getOrCreateTenantPool,
  pools,
};
