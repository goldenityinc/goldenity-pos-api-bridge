const ensuredTenantTables = new Set();

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

  await tenantDb.query(
    `ALTER TABLE "${normalizedTable}" ADD COLUMN IF NOT EXISTS tenant_id TEXT`,
  );
  await tenantDb.query(
    `UPDATE "${normalizedTable}" SET tenant_id = $1 WHERE tenant_id IS NULL`,
    [normalizedTenantId],
  );
  await tenantDb.query(
    `CREATE INDEX IF NOT EXISTS idx_${normalizedTable}_tenant_id ON "${normalizedTable}" (tenant_id)`,
  );

  ensuredTenantTables.add(cacheKey);
};

module.exports = {
  ensureTenantScopedTable,
};
