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

const PIVOT_CANDIDATES = [
  { table: 'user_branches', userColumn: 'user_id', branchColumn: 'branch_id' },
  { table: 'employee_branches', userColumn: 'user_id', branchColumn: 'branch_id' },
  { table: 'employee_branches', userColumn: 'employee_id', branchColumn: 'branch_id' },
  { table: 'app_user_branches', userColumn: 'user_id', branchColumn: 'branch_id' },
  { table: 'app_user_branches', userColumn: 'app_user_id', branchColumn: 'branch_id' },
  { table: 'users_branches', userColumn: 'user_id', branchColumn: 'branch_id' },
];

const resolvePivotConfig = async (db) => {
  for (const candidate of PIVOT_CANDIDATES) {
    // Pick the first pivot table shape that exists in this tenant schema.
    // This keeps the endpoint compatible across legacy naming variants.
    const exists = await tableExists(db, candidate.table);
    if (!exists) {
      continue;
    }

    const columns = await getTableColumns(db, candidate.table);
    if (!columns.has(candidate.userColumn) || !columns.has(candidate.branchColumn)) {
      continue;
    }

    return {
      ...candidate,
      hasTenantColumn: columns.has('tenant_id'),
    };
  }

  return null;
};

const getAssignedBranches = async (db, tenantId, userId) => {
  const pivot = await resolvePivotConfig(db);
  if (!pivot) {
    return [];
  }

  const values = [tenantId, userId];
  const tenantFilter = pivot.hasTenantColumn ? ' AND p.tenant_id = $1' : '';

  const result = await db.query(
    `SELECT b.*
     FROM branches b
     INNER JOIN ${pivot.table} p ON p.${pivot.branchColumn} = b.id
     WHERE b.tenant_id = $1
       AND p.${pivot.userColumn} = $2
       ${tenantFilter}
     ORDER BY name ASC NULLS LAST, id ASC`,
    values,
  );

  return result.rows;
};

const listBranchesForCurrentUser = async (req, res) => {
  try {
    const db = req?.tenantDb;
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
    const userId = resolveUserId(req);

    if (!roleProvidedInToken(req)) {
      const roleFromDb = await getAppUserRoleFromDb(db, tenantId, userId);
      if (roleFromDb) {
        role = roleFromDb;
      }
    }

    if (VIP_ROLES.has(role)) {
      const rows = await getAllTenantBranches(db, tenantId);
      return jsonOk(res, rows);
    }

    const rows = await getAssignedBranches(db, tenantId, userId);
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  listBranchesForCurrentUser,
};
