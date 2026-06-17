const { jsonOk, jsonError } = require('../utils/http');
const { normalizeTenantId } = require('../utils/sqlHelpers');

const resolveTenantIdFromRequest = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);

const normalizeRole = (value) => (value ?? '')
  .toString()
  .trim()
  .toUpperCase()
  .replace(/[\s-]+/g, '_');

const hasAuditLogAccess = (req) => {
  const role = normalizeRole(req?.user?.role || req?.auth?.role);
  return role === 'SUPER_ADMIN' || role === 'TENANT_ADMIN' || role === 'OWNER' || role === 'ADMIN';
};

const parseLimit = (value) => {
  const parsed = Number.parseInt((value ?? '').toString(), 10);
  if (!Number.isFinite(parsed)) return 100;
  return Math.max(1, Math.min(parsed, 500));
};

const listAuditLogs = async (req, res) => {
  try {
    if (!hasAuditLogAccess(req)) {
      return jsonError(res, 403, 'Akses ditolak: hanya Admin/Owner');
    }

    const tenantId = resolveTenantIdFromRequest(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    const limit = parseLimit(req.query?.limit);

    const result = await req.tenantDb.query(
      `SELECT
         al.id,
         al.tenant_id,
         al.user_id,
         COALESCE(al.user_name, u.name, al.user_id) AS user_name,
         al.action_type,
         al.details,
         al.created_at
       FROM audit_logs al
       LEFT JOIN users u ON u.id::text = al.user_id
       WHERE al.tenant_id = $1
       ORDER BY al.created_at DESC, al.id DESC
       LIMIT $2`,
      [tenantId, limit],
    );

    return jsonOk(res, result.rows, 'Success');
  } catch (error) {
    if (error?.code === '42P01') {
      return jsonOk(res, [], 'Success');
    }
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  listAuditLogs,
};
