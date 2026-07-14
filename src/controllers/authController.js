const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const http = require('http');
const https = require('https');
const { getSharedPool } = require('../middlewares/tenantResolver');
const { jsonOk, jsonError } = require('../utils/http');
const { normalizeSubscriptionAddons } = require('../constants/subscriptionAddons');
const { getTableColumnSet } = require('../utils/sqlHelpers');

const normalizeOptionalText = (value) => {
  if (value === undefined || value === null) {
    return null;
  }
  const text = value.toString().trim();
  return text || null;
};

const extractSubscriptionEndDateFromPayload = (payload = {}) => {
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const candidates = [
    payload.endDate,
    payload.subscription_end_date,
    payload.subscriptionEndDate,
    payload?.subscription?.endDate,
    payload?.subscription?.subscription_end_date,
    payload?.subscription?.subscriptionEndDate,
    payload?.data?.endDate,
    payload?.data?.subscription_end_date,
    payload?.data?.subscriptionEndDate,
    payload?.data?.subscription?.endDate,
    payload?.data?.subscription?.subscription_end_date,
    payload?.data?.subscription?.subscriptionEndDate,
    payload?.user?.endDate,
    payload?.user?.subscription_end_date,
    payload?.tenant?.endDate,
    payload?.tenant?.subscription_end_date,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeOptionalText(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return null;
};

const postJson = async (urlString, payload, timeoutMs = 8000) => {
  const url = new URL(urlString);
  const requestLib = url.protocol === 'https:' ? https : http;
  const body = JSON.stringify(payload || {});

  return new Promise((resolve, reject) => {
    const req = requestLib.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (response) => {
        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('end', () => {
          const raw = Buffer.concat(chunks).toString('utf8');
          let parsed = null;
          if (raw) {
            try {
              parsed = JSON.parse(raw);
            } catch (_) {
              parsed = null;
            }
          }

          resolve({
            statusCode: response.statusCode || 0,
            data: parsed,
          });
        });
      },
    );

    req.on('error', reject);
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error('Admin core login request timeout'));
    });

    req.write(body);
    req.end();
  });
};

const fetchAdminCoreAuthPayload = async ({ username, password, tenantId }) => {
  const baseUrl = (
    process.env.ADMIN_CORE_BASE_URL ||
    process.env.ADMIN_CORE_URL ||
    process.env.ADMIN_CORE_BACKEND_URL ||
    ''
  )
    .toString()
    .trim();

  if (!baseUrl) {
    return null;
  }

  try {
    const loginUrl = new URL('/auth/login', baseUrl).toString();
    const response = await postJson(loginUrl, { username, password, tenantId });

    if (response.statusCode < 200 || response.statusCode >= 300) {
      return null;
    }

    return response.data;
  } catch (error) {
    console.warn('[authController] Admin core login proxy skipped:', error.message);
    return null;
  }
};

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

    const adminCoreAuthPayload = await fetchAdminCoreAuthPayload({
      username,
      password,
      tenantId,
    });
    const proxiedSubscriptionEndDate = extractSubscriptionEndDateFromPayload(adminCoreAuthPayload);

    const appInstanceColumns = await getTableColumnSet(pool, 'app_instances');
    const endDateExpressions = [];
    if (appInstanceColumns.has('endDate')) {
      endDateExpressions.push('ai."endDate"::text');
    }
    if (appInstanceColumns.has('subscription_end_date')) {
      endDateExpressions.push('ai.subscription_end_date::text');
    }
    if (appInstanceColumns.has('end_date')) {
      endDateExpressions.push('ai.end_date::text');
    }

    const subscriptionResult = await pool.query(
      `
      SELECT
        ai."tier"::text AS tier,
        COALESCE(ai."addons", ARRAY[]::text[]) AS addons,
        ${endDateExpressions.length > 0
    ? `NULLIF(BTRIM(COALESCE(${endDateExpressions.join(', ')})), '')`
    : 'NULL::text'} AS subscription_end_date
      FROM app_instances ai
      LEFT JOIN solutions s ON s.id = ai."solutionId"
      WHERE ai."tenantId" = $1
        AND ai.status = 'ACTIVE'
      ORDER BY
        CASE
          WHEN UPPER(COALESCE(s.code, '')) = 'POS' THEN 0
          WHEN UPPER(COALESCE(s.name, '')) LIKE '%POS%' THEN 1
          ELSE 2
        END,
        ai."updatedAt" DESC
      LIMIT 1
      `,
      [tenantId],
    );

    const resolvedTier = subscriptionResult.rows[0]?.tier ?? null;
    const resolvedAddons = normalizeSubscriptionAddons(subscriptionResult.rows[0]?.addons);
    const dbSubscriptionEndDate = normalizeOptionalText(subscriptionResult.rows[0]?.subscription_end_date);
    const resolvedSubscriptionEndDate = proxiedSubscriptionEndDate ?? dbSubscriptionEndDate;

    const token = jwt.sign(
      {
        sub: user.id,
        username: user.username,
        tenantId,
        tier: resolvedTier,
        addons: resolvedAddons,
        subscriptionEndDate: resolvedSubscriptionEndDate,
        subscription_end_date: resolvedSubscriptionEndDate,
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
      user: {
        ...user,
        custom_role_permissions: customRolePermissions,
        tier: resolvedTier,
        addons: resolvedAddons,
        endDate: resolvedSubscriptionEndDate,
        subscription_end_date: resolvedSubscriptionEndDate,
      },
      subscription: {
        tier: resolvedTier,
        addons: resolvedAddons,
        endDate: resolvedSubscriptionEndDate,
        subscription_end_date: resolvedSubscriptionEndDate,
      },
      endDate: resolvedSubscriptionEndDate,
      subscription_end_date: resolvedSubscriptionEndDate,
      token,
    });
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  login,
};
