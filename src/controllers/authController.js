const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { getSharedPool } = require('../middlewares/tenantResolver');
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
    } = req.body || {};

    if (!username || !password || !tenantId) {
      return jsonError(res, 400, 'username, password, dan tenantId wajib diisi');
    }

    const pool = getSharedPool();
    const result = await pool.query(
      'SELECT * FROM app_users WHERE username = $1 AND tenant_id = $2 LIMIT 1',
      [username, tenantId],
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
      },
      jwtSecret,
      { expiresIn: process.env.JWT_EXPIRES_IN || '7d' },
    );

    // Jika user memiliki custom_role_id, sertakan permissions-nya
    // supaya POS app bisa menerapkan akses RBAC tanpa round-trip tambahan.
    let customRolePermissions = null;
    if (user.custom_role_id) {
      try {
        const roleResult = await pool.query(
          'SELECT permissions FROM custom_roles WHERE id = $1 AND tenant_id = $2 LIMIT 1',
          [user.custom_role_id, tenantId],
        );
        if (roleResult.rows[0]) {
          customRolePermissions = roleResult.rows[0].permissions;
        }
      } catch {
        // custom_roles table mungkin belum ada di tenant lama – skip
      }
    }

    return jsonOk(res, {
      user: { ...user, custom_role_permissions: customRolePermissions },
      token,
    });
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  login,
};
