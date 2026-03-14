const { jsonOk, jsonError } = require('../utils/http');
const {
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  buildUpsertQuery,
  runSelect,
} = require('../utils/sqlHelpers');

const ensureCustomersTable = async (tenantDb, table) => {
  if (table !== 'customers') return;
  await tenantDb.query(`
    CREATE TABLE IF NOT EXISTS customers (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      phone TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
};

const isUndefinedCustomersTableError = (error, table) => {
  if (table !== 'customers') return false;
  const message = (error?.message || '').toString().toLowerCase();
  return error?.code === '42P01' || message.includes('relation "customers" does not exist');
};

const runWithCustomersTableRetry = async (tenantDb, table, operation) => {
  await ensureCustomersTable(tenantDb, table);

  try {
    return await operation();
  } catch (error) {
    if (!isUndefinedCustomersTableError(error, table)) {
      throw error;
    }

    // Handle first-run race/legacy schema: create table then retry once.
    await ensureCustomersTable(tenantDb, table);
    return operation();
  }
};

const listRecords = async (req, res) => {
  try {
    const rows = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      () => runSelect(req.tenantDb, req.params.table, req.query),
    );
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createRecords = async (req, res) => {
  try {
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      async () => {
        const arrayPayload = parseBodyArray(req.body);
        const payload = arrayPayload || parseBodyObject(req.body);
        const { sql, values } = buildInsertQuery(req.params.table, payload);
        return req.tenantDb.query(sql, values);
      },
    );
    return jsonOk(res, result.rows, 'Created', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const upsertRecords = async (req, res) => {
  try {
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      async () => {
        const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
        const onConflict = req.body?.onConflict;
        const { sql, values } = buildUpsertQuery(req.params.table, payload, onConflict);
        return req.tenantDb.query(sql, values);
      },
    );
    return jsonOk(res, result.rows, 'Upserted');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const updateRecordById = async (req, res) => {
  try {
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      async () => {
        const idField = req.query.idField || 'id';
        const payload = parseBodyObject(req.body);
        const { sql, values } = buildUpdateQuery(req.params.table, payload, idField, req.params.id);
        return req.tenantDb.query(sql, values);
      },
    );

    if ((result.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Record tidak ditemukan');
    }
    return jsonOk(res, result.rows[0] || null, 'Updated');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const deleteRecordById = async (req, res) => {
  try {
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      async () => {
        const idField = req.query.idField || 'id';
        const { sql, values } = buildDeleteQuery(req.params.table, idField, req.params.id);
        return req.tenantDb.query(sql, values);
      },
    );
    return jsonOk(res, result.rows[0] || null, 'Deleted');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  listRecords,
  createRecords,
  upsertRecords,
  updateRecordById,
  deleteRecordById,
};
