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

    if (!tenantId || typeof tenantId !== 'string') {
      return res.status(401).json({
        success: false,
        message: 'tenantId tidak ditemukan di token',
        error: null,
      });
    }

    const pool = getSharedPool();
    await pool.query('SELECT 1');

    req.auth = payload;
    req.tenant = { tenantId };
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
