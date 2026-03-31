const { jsonOk, jsonError } = require('../utils/http');
const { randomUUID } = require('crypto');
const {
  normalizeArray,
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  buildUpsertQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
  isTenantScopedTable,
} = require('../utils/sqlHelpers');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const { emitTableMutation } = require('../services/realtimeEmitter');

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

const normalizeProductPayload = (payload = {}, { isCreate = false } = {}) => {
  const next = { ...payload };

  if (next.imageUrl !== undefined && next.image_url === undefined) {
    next.image_url = next.imageUrl;
  }

  if (isCreate && (next.id === undefined || next.id === null || `${next.id}`.trim() === '')) {
    next.id = randomUUID();
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
    return normalizeProductPayload(payload, options);
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
  tenantId,
}) => {
  if (table !== 'products') {
    return;
  }

  const existing = await runSelect(tenantDb, table, {
    [`eq__${idField}`]: idValue,
    maybeSingle: true,
  }, { tenantId });

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

  await tenantDb.query(`
    ALTER TABLE products
    ADD COLUMN IF NOT EXISTS reference_id TEXT;
  `);

  await tenantDb.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_products_reference_id_unique
    ON products (reference_id)
    WHERE reference_id IS NOT NULL;
  `);
};

const findExistingRecordByReferenceId = async (tenantDb, table, payload, tenantId) => {
  if (table !== 'products' || Array.isArray(payload) || !payload) {
    return null;
  }

  const referenceId = (
    payload.reference_id ?? payload.referenceId ?? payload.local_id ?? payload.localId ?? ''
  )
    .toString()
    .trim();
  if (!referenceId) {
    return null;
  }

  return runSelect(tenantDb, table, {
    eq__reference_id: referenceId,
    maybeSingle: true,
  }, { tenantId });
};

const isUndefinedCustomersTableError = (error, table) => {
  if (table !== 'customers') return false;
  const message = (error?.message || '').toString().toLowerCase();
  return error?.code === '42P01' || message.includes('relation "customers" does not exist');
};

const runWithCustomersTableRetry = async (tenantDb, table, tenantId, operation) => {
  if (isTenantScopedTable(table)) {
    await ensureTenantScopedTable(tenantDb, table, tenantId);
  }

  await ensureCustomersTable(tenantDb, table);
  await ensureProductsTableColumns(tenantDb, table);

  try {
    return await operation();
  } catch (error) {
    if (!isUndefinedCustomersTableError(error, table)) {
      throw error;
    }

    // Handle first-run race/legacy schema: create table then retry once.
    if (isTenantScopedTable(table)) {
      await ensureTenantScopedTable(tenantDb, table, tenantId);
    }
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

const hasMutationFields = (payload) => {
  if (Array.isArray(payload)) {
    return payload.some(
      (row) => row && typeof row === 'object' && Object.keys(row).length > 0,
    );
  }

  return !!payload && typeof payload === 'object' && Object.keys(payload).length > 0;
};

const resolveTenantId = (req) => normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
const AGGREGATION_TABLES = new Set(['sales_records', 'expenses', 'daily_cash', 'petty_cash_logs']);

const normalizeSalesRecordId = (value) => {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseItemsFromJsonField = (value) => {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return [];
    }
  }

  return [];
};

const loadSalesRecordItemsMap = async (tenantDb, rows, tenantId) => {
  const ids = rows
    .map((row) => normalizeSalesRecordId(row?.id))
    .filter((id) => id !== null);

  if (ids.length === 0) {
    return new Map();
  }

  try {
    const normalizedTenantId = normalizeTenantId(tenantId);
    const result = await tenantDb.query(
      normalizedTenantId
        ? `SELECT sales_record_id, product_id, product_name, qty, custom_price, note, is_service
           FROM sales_record_items
           WHERE tenant_id = $1
             AND sales_record_id = ANY($2::bigint[])
           ORDER BY id ASC`
        : `SELECT sales_record_id, product_id, product_name, qty, custom_price, note, is_service
           FROM sales_record_items
           WHERE sales_record_id = ANY($1::bigint[])
           ORDER BY id ASC`,
      normalizedTenantId ? [normalizedTenantId, ids] : [ids],
    );

    const grouped = new Map();
    for (const row of result.rows || []) {
      const key = Number(row.sales_record_id);
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push({
        product_id: row.product_id || null,
        product_name: row.product_name || '',
        qty: Number(row.qty || 0),
        custom_price: row.custom_price === null ? null : Number(row.custom_price),
        note: (row.note || '').toString(),
        is_service: row.is_service === true,
      });
    }

    return grouped;
  } catch (_) {
    return new Map();
  }
};

const listRecords = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);

    if (table === 'sales_records') {
      const primaryKeyColumn = await resolvePrimaryKeyColumn(req.tenantDb, table);
      const query = {
        ...req.query,
        select: ensureSelectedColumn(req.query?.select, primaryKeyColumn),
      };

      const rows = await runWithCustomersTableRetry(
        req.tenantDb,
        table,
        tenantId,
        () => runSelect(req.tenantDb, table, query, { tenantId }),
      );

      const normalizedRows = Array.isArray(rows)
        ? rows.map((row) => ({
          ...row,
          id: row?.id ?? row?.[primaryKeyColumn] ?? null,
        }))
        : rows;

      const itemsMap = await loadSalesRecordItemsMap(
        req.tenantDb,
        Array.isArray(normalizedRows) ? normalizedRows : [],
        tenantId,
      );

      const hydratedRows = Array.isArray(normalizedRows)
        ? normalizedRows.map((row) => {
          const rowId = normalizeSalesRecordId(row?.id);
          const relationItems = rowId !== null ? itemsMap.get(rowId) || [] : [];
          return {
            ...row,
            items: relationItems.length > 0
              ? relationItems
              : parseItemsFromJsonField(row?.items_json ?? row?.items),
          };
        })
        : normalizedRows;

      return jsonOk(res, hydratedRows);
    }

    let effectiveQuery = { ...req.query };
    if (effectiveQuery.orderBy) {
      const currentColumns = await getTableColumnSet(req.tenantDb, table);
      const requestedOrderBy = (effectiveQuery.orderBy || '').toString().trim();
      if (requestedOrderBy && !currentColumns.has(requestedOrderBy)) {
        if (currentColumns.has('created_at')) {
          effectiveQuery.orderBy = 'created_at';
        } else if (currentColumns.has('updated_at')) {
          effectiveQuery.orderBy = 'updated_at';
        } else if (currentColumns.has('id')) {
          effectiveQuery.orderBy = 'id';
        } else {
          delete effectiveQuery.orderBy;
        }
      }
    }
    if (table === 'customers' && !effectiveQuery.select) {
      const customerColumns = await getTableColumnSet(req.tenantDb, table);
      const preferredColumns = [
        'id',
        'name',
        'phone',
        'address',
        'points',
        'total_spent',
        'created_at',
        'updated_at',
      ].filter((column) => customerColumns.has(column));

      if (preferredColumns.length > 0) {
        effectiveQuery.select = preferredColumns.join(',');
      }
      if (!effectiveQuery.limit) {
        effectiveQuery.limit = 500;
      }
    }

    const rows = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      () => runSelect(req.tenantDb, table, effectiveQuery, { tenantId }),
    );
    return jsonOk(res, rows);
  } catch (error) {
    const table = (req?.params?.table || '').toString().trim().toLowerCase();
    if (AGGREGATION_TABLES.has(table)) {
      console.error('Dashboard Aggregation Error:', error);
    }
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createRecords = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const arrayPayload = parseBodyArray(req.body);
        const payload = normalizePayloadForTable(
          table,
          arrayPayload || parseBodyObject(req.body),
          { isCreate: true },
        );
        const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
        const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
        const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
        if (!hasMutationFields(filteredPayload)) {
          throw createHttpError(400, `Tidak ada kolom yang cocok untuk tabel ${table}`);
        }
        const existingRecord = await findExistingRecordByReferenceId(
          req.tenantDb,
          table,
          filteredPayload,
          tenantId,
        );
        if (existingRecord) {
          return {
            rows: [existingRecord],
            rowCount: 1,
            idempotentHit: true,
          };
        }
        validateProductCreateOrUpsertPayload(table, filteredPayload);
        const { sql, values } = buildInsertQuery(table, filteredPayload);
        return req.tenantDb.query(sql, values);
      },
    );
    if (!result.idempotentHit) {
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'INSERT',
          record: row,
        });
      }
    }
    return jsonOk(
      res,
      result.rows,
      result.idempotentHit ? 'Already exists' : 'Created',
      result.idempotentHit ? 200 : 201,
    );
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
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const payload = normalizePayloadForTable(
          table,
          parseBodyArray(req.body) || parseBodyObject(req.body),
          { isCreate: true },
        );
        const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
        const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
        const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
        if (!hasMutationFields(filteredPayload)) {
          throw createHttpError(400, `Tidak ada kolom yang cocok untuk tabel ${table}`);
        }
        const existingRecord = await findExistingRecordByReferenceId(
          req.tenantDb,
          table,
          filteredPayload,
          tenantId,
        );
        if (existingRecord) {
          return {
            rows: [existingRecord],
            rowCount: 1,
            idempotentHit: true,
          };
        }
        validateProductCreateOrUpsertPayload(table, filteredPayload);
        const onConflict = req.body?.onConflict;
        const filteredOnConflict = normalizeArray(onConflict).filter((column) =>
          columnDefinitions.has(column),
        );
        if (filteredOnConflict.length === 0) {
          throw createHttpError(400, 'onConflict tidak cocok dengan schema tabel');
        }
        const { sql, values } = buildUpsertQuery(
          table,
          filteredPayload,
          filteredOnConflict,
        );
        return req.tenantDb.query(sql, values);
      },
    );
    if (!result.idempotentHit) {
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'UPSERT',
          record: row,
        });
      }
    }
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
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const idField = req.query.idField || 'id';
        const payload = normalizePayloadForTable(
          table,
          parseBodyObject(req.body),
          { isCreate: false },
        );
        const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
        const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
        const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
        await validateProductUpdatePayload({
          tenantDb: req.tenantDb,
          table,
          idField,
          idValue: req.params.id,
          payload: filteredPayload,
          tenantId,
        });
        if (!hasMutationFields(filteredPayload)) {
          const existing = await runSelect(req.tenantDb, table, {
            [`eq__${idField}`]: req.params.id,
            maybeSingle: true,
          }, { tenantId });
          return {
            rowCount: existing ? 1 : 0,
            rows: existing ? [existing] : [],
          };
        }
        const columnSet = await getTableColumnSet(req.tenantDb, table);
        const { sql, values } = buildUpdateQuery(
          table,
          filteredPayload,
          idField,
          req.params.id,
          { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
        );
        return req.tenantDb.query(sql, values);
      },
    );

    if ((result.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Record tidak ditemukan');
    }
    emitTableMutation(req, {
      table,
      action: 'UPDATE',
      record: result.rows[0] || null,
      id: req.params.id,
    });
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
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const idField = req.query.idField || 'id';
        const columnSet = await getTableColumnSet(req.tenantDb, table);
        const { sql, values } = buildDeleteQuery(
          table,
          idField,
          req.params.id,
          { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
        );
        return req.tenantDb.query(sql, values);
      },
    );
    emitTableMutation(req, {
      table,
      action: 'DELETE',
      record: result.rows[0] || null,
      id: req.params.id,
    });
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
