const ensuredTenantTables = new Set();
const TENANT_COLUMN = 'tenant_id';

const isSafeIdentifier = (value) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);

const ensureTenantScopedTable = async (tenantDb, table, tenantId) => {
  const normalizedTable = (table || '').toString().trim();
  const normalizedTenantId = (tenantId || '').toString().trim();

  if (!isSafeIdentifier(normalizedTable)) {
    throw new Error(`Security guard: invalid table identifier ${table}`);
  }
  if (!normalizedTenantId) {
    throw new Error('Security guard: tenantId wajib tersedia');
  }

  const cacheKey = `${normalizedTable}::${normalizedTenantId}`;
  if (ensuredTenantTables.has(cacheKey)) {
    return;
  }

  const result = await tenantDb.query(
    `SELECT 1
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1
       AND column_name = $2
     LIMIT 1`,
    [normalizedTable, TENANT_COLUMN],
  );

  if ((result.rowCount || 0) === 0) {
    throw new Error(
      `Security guard: tabel ${normalizedTable} wajib memiliki kolom ${TENANT_COLUMN} pada shared database`,
    );
  }

  ensuredTenantTables.add(cacheKey);
};

module.exports = {
  ensureTenantScopedTable,
};
