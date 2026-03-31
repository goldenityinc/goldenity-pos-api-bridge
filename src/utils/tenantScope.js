const ensuredTenantTables = new Set();
const TENANT_COLUMN = 'tenant_id';
const LEGACY_BACKFILL_TABLES = new Set([
  'sales_records',
  'expenses',
  'daily_cash',
  'petty_cash_logs',
]);

const isSafeIdentifier = (value) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);

const maybeBackfillLegacyTenantRows = async (tenantDb, table, tenantId) => {
  if (!LEGACY_BACKFILL_TABLES.has(table)) {
    return;
  }

  const statsResult = await tenantDb.query(
    `SELECT
       COUNT(*) FILTER (WHERE ${TENANT_COLUMN} IS NULL OR TRIM(${TENANT_COLUMN}) = '') AS null_rows,
       COUNT(DISTINCT ${TENANT_COLUMN}) FILTER (WHERE ${TENANT_COLUMN} IS NOT NULL AND TRIM(${TENANT_COLUMN}) <> '') AS distinct_tenant_rows
     FROM "${table}"`,
  );

  const nullRows = Number(statsResult.rows?.[0]?.null_rows || 0);
  const distinctTenantRows = Number(statsResult.rows?.[0]?.distinct_tenant_rows || 0);

  // Safe transitional backfill:
  // - only when historical rows are still unscoped (null tenant)
  // - and there is no existing multi-tenant split in that table yet
  if (nullRows > 0 && distinctTenantRows === 0) {
    await tenantDb.query(
      `UPDATE "${table}"
       SET ${TENANT_COLUMN} = $1
       WHERE ${TENANT_COLUMN} IS NULL OR TRIM(${TENANT_COLUMN}) = ''`,
      [tenantId],
    );
  }
};

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
    // Self-heal for shared DB drift: add tenant column if missing.
    await tenantDb.query(`ALTER TABLE "${normalizedTable}" ADD COLUMN IF NOT EXISTS ${TENANT_COLUMN} TEXT`);
    await tenantDb.query(
      `CREATE INDEX IF NOT EXISTS idx_${normalizedTable}_${TENANT_COLUMN} ON "${normalizedTable}" (${TENANT_COLUMN})`,
    );

    const recheck = await tenantDb.query(
      `SELECT 1
       FROM information_schema.columns
       WHERE table_schema = ANY(current_schemas(false))
         AND table_name = $1
         AND column_name = $2
       LIMIT 1`,
      [normalizedTable, TENANT_COLUMN],
    );

    if ((recheck.rowCount || 0) === 0) {
      throw new Error(
        `Security guard: tabel ${normalizedTable} wajib memiliki kolom ${TENANT_COLUMN} pada shared database`,
      );
    }
  }

  await maybeBackfillLegacyTenantRows(tenantDb, normalizedTable, normalizedTenantId);

  ensuredTenantTables.add(cacheKey);
};

module.exports = {
  ensureTenantScopedTable,
};
