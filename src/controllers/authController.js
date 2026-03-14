const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { getOrCreateTenantPool } = require('../middlewares/tenantResolver');
const { jsonOk, jsonError } = require('../utils/http');

const login = async (req, res) => {
  try {
    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret) {
      return jsonError(res, 500, 'JWT_SECRET belum dikonfigurasi');
    }

    const {
      username,
      password,
      tenantId,
      dbUrl,
    } = req.body || {};

    if (!username || !password || !tenantId || !dbUrl) {
      return jsonError(res, 400, 'username, password, tenantId, dan dbUrl wajib diisi');
    }

    const pool = getOrCreateTenantPool(tenantId, dbUrl);
    const result = await pool.query(
      'SELECT * FROM app_users WHERE username = $1 LIMIT 1',
      [username],
    );

    const user = result.rows[0];
    if (!user) {
      return jsonError(res, 401, 'Login gagal', 'Unauthorized');
    }

    // Support both bcrypt-hashed passwords and legacy plain-text passwords
    const storedPassword = user.password || '';
    const isBcrypt = storedPassword.startsWith('$2a$') || storedPassword.startsWith('$2b$');
    const passwordMatch = isBcrypt
      ? await bcrypt.compare(password, storedPassword)
      : storedPassword === password;

    if (!passwordMatch) {
      return jsonError(res, 401, 'Login gagal', 'Unauthorized');
    }

    const token = jwt.sign(
      {
        sub: user.id,
        username: user.username,
        tenantId,
        dbUrl,
      },
      jwtSecret,
      { expiresIn: process.env.JWT_EXPIRES_IN || '7d' },
    );

    return jsonOk(res, { user, token });
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  login,
};
