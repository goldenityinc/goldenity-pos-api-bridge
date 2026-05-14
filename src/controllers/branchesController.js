const { jsonOk, jsonError } = require('../utils/http');

const normalizeText = (value) => (value ?? '').toString().trim();

const resolveTenantId = (req) => normalizeText(
  req?.user?.tenantId
  || req?.user?.tenant_id
  || req?.tenant?.tenantId
  || req?.auth?.tenantId
  || req?.auth?.tenant_id,
);

const resolveUserId = (req) => normalizeText(
  req?.user?.userId
  || req?.user?.user_id
  || req?.user?.id
  || req?.auth?.userId
  || req?.auth?.user_id
  || req?.auth?.id
  || req?.auth?.sub,
);

const normalizeRole = (value) => normalizeText(value).toLowerCase().replace(/[\s-]+/g, '_');

const VIP_ROLES = new Set([
  'owner',
  'superadmin',
  'super_admin',
  'admin',
  'tenant_admin',
  'tenantadmin',
]);

const getAuthRole = (req) => {
  const candidates = [
    req?.auth?.role,
    req?.auth?.userRole,
    req?.auth?.user_role,
    req?.auth?.appRole,
    req?.auth?.app_role,
    req?.user?.role,
    req?.user?.userRole,
    req?.user?.user_role,
    req?.user?.appRole,
    req?.user?.app_role,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeRole(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return '';
};

const roleProvidedInToken = (req) => {
  const tokenCandidates = [
    req?.auth?.role,
    req?.auth?.userRole,
    req?.auth?.user_role,
    req?.auth?.appRole,
    req?.auth?.app_role,
  ];

  return tokenCandidates.some((candidate) => normalizeRole(candidate));
};

const getAppUserRoleFromDb = async (db, tenantId, userId) => {
  if (!tenantId || !userId) {
    return '';
  }

  const result = await db.query(
    'SELECT role FROM app_users WHERE id = $1 AND tenant_id = $2 LIMIT 1',
    [userId, tenantId],
  );

  return normalizeRole(result.rows[0]?.role);
};

const tableExists = async (db, tableName) => {
  const result = await db.query(
    `SELECT EXISTS (
       SELECT 1
       FROM information_schema.tables
       WHERE table_schema = ANY(current_schemas(false))
         AND table_name = $1
     ) AS exists`,
    [tableName],
  );

  return result.rows[0]?.exists === true;
};

const getTableColumns = async (db, tableName) => {
  const result = await db.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1`,
    [tableName],
  );

  return new Set(result.rows.map((row) => row.column_name));
};

const getAllTenantBranches = async (db, tenantId) => {
  const result = await db.query(
    `SELECT *
     FROM branches
     WHERE tenant_id = $1
     ORDER BY name ASC NULLS LAST, id ASC`,
    [tenantId],
  );

  return result.rows;
};

const resolveBranchIdFromToken = (req) => normalizeText(
  req?.user?.branch_id
  || req?.user?.branchId
  || req?.auth?.branch_id
  || req?.auth?.branchId,
);

const USER_TABLE_CANDIDATES = ['app_users', 'users'];

const getUserBranchIdFromDb = async (db, tenantId, userId) => {
  if (!tenantId || !userId) {
    return '';
  }

  for (const tableName of USER_TABLE_CANDIDATES) {
    const exists = await tableExists(db, tableName);
    if (!exists) {
      continue;
    }

    const columns = await getTableColumns(db, tableName);
    if (!columns.has('branch_id') || !columns.has('id')) {
      continue;
    }

    if (columns.has('tenant_id')) {
      const withTenant = await db.query(
        `SELECT branch_id
         FROM ${tableName}
         WHERE id = $1 AND tenant_id = $2
         LIMIT 1`,
        [userId, tenantId],
      );
      const branchId = normalizeText(withTenant.rows[0]?.branch_id);
      if (branchId) {
        return branchId;
      }
      continue;
    }

    const withoutTenant = await db.query(
      `SELECT branch_id
       FROM ${tableName}
       WHERE id = $1
       LIMIT 1`,
      [userId],
    );
    const branchId = normalizeText(withoutTenant.rows[0]?.branch_id);
    if (branchId) {
      return branchId;
    }
  }

  return '';
};

const getBranchById = async (db, tenantId, branchId) => {
  if (!tenantId || !branchId) {
    return [];
  }

  const result = await db.query(
    `SELECT *
     FROM branches
     WHERE tenant_id = $1 AND id = $2
     LIMIT 1`,
    [tenantId, branchId],
  );

  if (!result.rows.length) {
    return [];
  }

  return [result.rows[0]];
};

const listBranchesForCurrentUser = async (req, res) => {
  try {
    const userId = resolveUserId(req);
    const db = req?.tenantDb;

    // === AGGRESSIVE LOGGING: Log every request ===
    console.log('[BRANCH API] Request from User:', userId, '| Role:', getAuthRole(req));

    if (!db) {
      return jsonError(res, 500, 'Database tenant tidak tersedia');
    }

    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      return jsonError(res, 401, 'tenantId tidak ditemukan pada request');
    }

    const branchesTableExists = await tableExists(db, 'branches');
    if (!branchesTableExists) {
      return jsonOk(res, []);
    }

    let role = getAuthRole(req);

    if (!roleProvidedInToken(req)) {
      const roleFromDb = await getAppUserRoleFromDb(db, tenantId, userId);
      if (roleFromDb) {
        role = roleFromDb;
      }
    }

    if (VIP_ROLES.has(role)) {
      console.log(`[BRANCH API] VIP role "${role}" for user ${userId} → returning ALL tenant branches`);
      const rows = await getAllTenantBranches(db, tenantId);
      return jsonOk(res, rows);
    }

    // === STAFF/CASHIER FLOW: STRICT GUARDRAIL ===
    console.log(`[BRANCH API] Staff/Cashier user ${userId} with role "${role}" → resolving branchId...`);

    let branchId = resolveBranchIdFromToken(req);
    if (!branchId) {
      branchId = await getUserBranchIdFromDb(db, tenantId, userId);
    }

    // === CRITICAL NULL CHECK: MUST FAIL SAFELY ===
    if (!branchId) {
      console.log(`[BRANCH API] SECURITY GUARD: Staff user ${userId} has no valid branchId. Returning empty array.`);
      return jsonOk(res, []);
    }

    console.log(`[BRANCH API] Resolved Staff BranchId for user ${userId}: "${branchId}"`);

    const rows = await getBranchById(db, tenantId, branchId);
    console.log(`[BRANCH API] Query returned ${rows.length} branch(es) for user ${userId}`);

    return jsonOk(res, rows);
  } catch (error) {
    console.error(`[BRANCH API] ERROR for user ${resolveUserId(req)}:`, error.message);
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  listBranchesForCurrentUser,
};
