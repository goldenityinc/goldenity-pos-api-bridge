const { jsonOk, jsonError } = require('../utils/http');

const normalizeRole = (value) => {
  return (value ?? '')
    .toString()
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '_');
};

const hasTableInCurrentSchema = async (client, tableName) => {
  const result = await client.query(
    `SELECT 1
     FROM information_schema.tables
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1
     LIMIT 1`,
    [tableName],
  );

  return (result.rowCount || 0) > 0;
};

const requireSuperAdminRole = (req, res) => {
  const normalizedRole = normalizeRole(
    req?.auth?.role ?? req?.auth?.userRole ?? req?.auth?.appRole,
  );

  if (normalizedRole === 'SUPER_ADMIN' || normalizedRole === 'SUPERADMIN') {
    return true;
  }

  jsonError(res, 403, 'Hanya SuperAdmin yang dapat menjalankan reset data');
  return false;
};

const resetOperationalData = async (req, res) => {
  if (!requireSuperAdminRole(req, res)) {
    return;
  }

  const client = await req.tenantDb.connect();

  try {
    const tablesInDeleteOrder = [
      'transaction_items',
      'kas_bon_payment_history',
      'sales_records',
      'products',
      'categories',
      'petty_cash',
      'daily_cash',
      'sync_queue',
    ];

    const deletedCounts = {};

    await client.query('BEGIN');

    for (const tableName of tablesInDeleteOrder) {
      const exists = await hasTableInCurrentSchema(client, tableName);
      if (!exists) {
        deletedCounts[tableName] = 0;
        continue;
      }

      const result = await client.query(`DELETE FROM "${tableName}"`);
      deletedCounts[tableName] = result.rowCount || 0;
    }

    await client.query('COMMIT');
    return jsonOk(
      res,
      {
        deletedCounts,
        protectedTables: ['app_users', 'tenant', 'store_settings'],
      },
      'Data operasional berhasil dibersihkan',
    );
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

module.exports = {
  resetOperationalData,
};