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

const createHttpError = (statusCode, message) => {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
};

const parseMoneyValue = (value) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  const normalized = value
    .toString()
    .replaceAll('.', '')
    .replaceAll(',', '')
    .trim();

  if (!normalized) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
};

const extractSellingPrice = (payload = {}) => parseMoneyValue(
  payload.price ?? payload.selling_price ?? payload.harga_jual,
);

const extractPurchasePrice = (payload = {}) => parseMoneyValue(
  payload.purchase_price ?? payload.cost_price ?? payload.harga_modal,
);

const assertSellingPriceFloor = ({ sellingPrice, purchasePrice }) => {
  if (sellingPrice === null || purchasePrice === null) {
    return;
  }

  if (sellingPrice < purchasePrice) {
    throw createHttpError(400, 'Gagal: Harga jual di bawah harga modal.');
  }
};

const validateProductCreateOrUpsertPayload = (table, payload) => {
  if (table !== 'products') {
    return;
  }

  const rows = Array.isArray(payload) ? payload : [payload];
  for (const row of rows) {
    assertSellingPriceFloor({
      sellingPrice: extractSellingPrice(row),
      purchasePrice: extractPurchasePrice(row),
    });
  }
};

const normalizeProductPayload = (payload = {}) => {
  const next = { ...payload };

  if (next.imageUrl !== undefined && next.image_url === undefined) {
    next.image_url = next.imageUrl;
  }

  delete next.imageUrl;
  return next;
};

const normalizeCustomerPayload = (payload = {}, { isCreate = false } = {}) => {
  const next = { ...payload };

  delete next.total_spent;
  delete next.totalSpent;

  // total_spent harus dikelola dari transaksi, bukan input manual create/update.
  if (isCreate) {
    next.total_spent = 0;
  }
  return next;
};

const normalizePayloadForTable = (table, payload, options = {}) => {
  if (Array.isArray(payload)) {
    return payload.map((row) => normalizePayloadForTable(table, row, options));
  }

  if (!payload || typeof payload !== 'object') {
    return payload;
  }

  if (table === 'products') {
    return normalizeProductPayload(payload);
  }

  if (table === 'customers') {
    return normalizeCustomerPayload(payload, options);
  }

  return payload;
};

const validateProductUpdatePayload = async ({
  tenantDb,
  table,
  idField,
  idValue,
  payload,
}) => {
  if (table !== 'products') {
    return;
  }

  const existing = await runSelect(tenantDb, table, {
    [`eq__${idField}`]: idValue,
    maybeSingle: true,
  });

  if (!existing) {
    throw createHttpError(404, 'Record tidak ditemukan');
  }

  const sellingPrice = extractSellingPrice(payload) ?? extractSellingPrice(existing);
  const purchasePrice = extractPurchasePrice(payload) ?? extractPurchasePrice(existing);

  assertSellingPriceFloor({ sellingPrice, purchasePrice });
};

const ensureCustomersTable = async (tenantDb, table) => {
  if (table !== 'customers') return;
  await tenantDb.query(`
    CREATE TABLE IF NOT EXISTS customers (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      phone TEXT,
      total_spent DOUBLE PRECISION DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await tenantDb.query(`
    ALTER TABLE customers
    ADD COLUMN IF NOT EXISTS total_spent DOUBLE PRECISION DEFAULT 0;
  `);
};

const ensureProductsTableColumns = async (tenantDb, table) => {
  if (table !== 'products') return;
  await tenantDb.query(`
    ALTER TABLE products
    ADD COLUMN IF NOT EXISTS image_url TEXT;
  `);
};

const isUndefinedCustomersTableError = (error, table) => {
  if (table !== 'customers') return false;
  const message = (error?.message || '').toString().toLowerCase();
  return error?.code === '42P01' || message.includes('relation "customers" does not exist');
};

const runWithCustomersTableRetry = async (tenantDb, table, operation) => {
  await ensureCustomersTable(tenantDb, table);
  await ensureProductsTableColumns(tenantDb, table);

  try {
    return await operation();
  } catch (error) {
    if (!isUndefinedCustomersTableError(error, table)) {
      throw error;
    }

    // Handle first-run race/legacy schema: create table then retry once.
    await ensureCustomersTable(tenantDb, table);
    await ensureProductsTableColumns(tenantDb, table);
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
        const payload = normalizePayloadForTable(
          req.params.table,
          arrayPayload || parseBodyObject(req.body),
          { isCreate: true },
        );
        validateProductCreateOrUpsertPayload(req.params.table, payload);
        const { sql, values } = buildInsertQuery(req.params.table, payload);
        return req.tenantDb.query(sql, values);
      },
    );
    return jsonOk(res, result.rows, 'Created', 201);
  } catch (error) {
    return jsonError(
      res,
      error.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
  }
};

const upsertRecords = async (req, res) => {
  try {
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      async () => {
        const payload = normalizePayloadForTable(
          req.params.table,
          parseBodyArray(req.body) || parseBodyObject(req.body),
          { isCreate: true },
        );
        validateProductCreateOrUpsertPayload(req.params.table, payload);
        const onConflict = req.body?.onConflict;
        const { sql, values } = buildUpsertQuery(req.params.table, payload, onConflict);
        return req.tenantDb.query(sql, values);
      },
    );
    return jsonOk(res, result.rows, 'Upserted');
  } catch (error) {
    return jsonError(
      res,
      error.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
  }
};

const updateRecordById = async (req, res) => {
  try {
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      req.params.table,
      async () => {
        const idField = req.query.idField || 'id';
        const payload = normalizePayloadForTable(
          req.params.table,
          parseBodyObject(req.body),
          { isCreate: false },
        );
        await validateProductUpdatePayload({
          tenantDb: req.tenantDb,
          table: req.params.table,
          idField,
          idValue: req.params.id,
          payload,
        });
        const { sql, values } = buildUpdateQuery(req.params.table, payload, idField, req.params.id);
        return req.tenantDb.query(sql, values);
      },
    );

    if ((result.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Record tidak ditemukan');
    }
    return jsonOk(res, result.rows[0] || null, 'Updated');
  } catch (error) {
    return jsonError(
      res,
      error.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
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
