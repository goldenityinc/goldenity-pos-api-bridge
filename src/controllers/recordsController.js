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

const ensureSelectedColumn = (selectValue, columnName) => {
  if (!selectValue || selectValue === '*') {
    return selectValue;
  }

  const columns = selectValue
    .toString()
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean);

  const hasColumn = columns.some((column) => column.toLowerCase() === columnName.toLowerCase());
  if (hasColumn) {
    return columns.join(',');
  }

  return `${columns.join(',')},${columnName}`;
};

const resolvePrimaryKeyColumn = async (tenantDb, table) => {
  const result = await tenantDb.query(
    `SELECT kcu.column_name
     FROM information_schema.table_constraints tc
     JOIN information_schema.key_column_usage kcu
       ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
      AND tc.table_name = kcu.table_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = ANY(current_schemas(false))
      AND tc.table_name = $1
    ORDER BY kcu.ordinal_position
    LIMIT 1`,
    [table],
  );

  return result.rows[0]?.column_name || 'id';
};

const listRecords = async (req, res) => {
  try {
    const table = req.params.table;

    if (table === 'sales_records') {
      const primaryKeyColumn = await resolvePrimaryKeyColumn(req.tenantDb, table);
      const query = {
        ...req.query,
        select: ensureSelectedColumn(req.query?.select, primaryKeyColumn),
      };

      const rows = await runWithCustomersTableRetry(
        req.tenantDb,
        table,
        () => runSelect(req.tenantDb, table, query),
      );

      const normalizedRows = Array.isArray(rows)
        ? rows.map((row) => ({
          ...row,
          id: row?.id ?? row?.[primaryKeyColumn] ?? null,
        }))
        : rows;

      return jsonOk(res, normalizedRows);
    }

    const rows = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      () => runSelect(req.tenantDb, table, req.query),
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
